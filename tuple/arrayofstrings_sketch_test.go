/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tuple

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
)

func TestGenerateHashKeyFromStrings_UTF16LE(t *testing.T) {
	testCases := []struct {
		name string
		key  []string
		// want is the exact byte sequence Java's Util.stringArrHash hashes.
		want []byte
	}{
		{name: "nil", key: nil, want: nil},
		{name: "empty string", key: []string{""}, want: nil},
		{name: "ascii with separator", key: []string{"a", "b"}, want: []byte{0x61, 0x00, 0x2c, 0x00, 0x62, 0x00}},
		{name: "bmp", key: []string{"키"}, want: []byte{0xa4, 0xd0}},
		{name: "surrogate pair", key: []string{"🔑"}, want: []byte{0x3d, 0xd8, 0x11, 0xdd}},
		{
			name: "mixed with trailing empty element",
			key:  []string{"a", "🔑", ""},
			want: []byte{0x61, 0x00, 0x2c, 0x00, 0x3d, 0xd8, 0x11, 0xdd, 0x2c, 0x00},
		},
		{name: "invalid utf-8 byte", key: []string{"\xff"}, want: []byte{0xfd, 0xff}},
		{
			name: "longer than the stack buffer",
			key:  []string{strings.Repeat("ab", 200)},
			want: bytes.Repeat([]byte{0x61, 0x00, 0x62, 0x00}, 200),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hasher := xxhash.NewWithSeed(stringSliceHashSeed)
			hasher.Write(tc.want) //nolint:errcheck

			assert.Equal(t, hasher.Sum64(), GenerateHashKeyFromStrings(tc.key))
		})
	}
}

func TestGenerateHashKeyFromStrings_JavaDecoding(t *testing.T) {
	testCases := []struct {
		name string
		s    string
		// want is what new String(bytes, StandardCharsets.UTF_8) gives in Java 8-26.
		want []uint16
	}{
		{name: "last code point before surrogates", s: "\xed\x9f\xbf", want: []uint16{0xd7ff}},
		{name: "first code point after surrogates", s: "\xee\x80\x80", want: []uint16{0xe000}},
		{name: "replacement character", s: "\xef\xbf\xbd", want: []uint16{0xfffd}},
		{name: "first supplementary code point", s: "\xf0\x90\x80\x80", want: []uint16{0xd800, 0xdc00}},
		{name: "last code point", s: "\xf4\x8f\xbf\xbf", want: []uint16{0xdbff, 0xdfff}},
		{name: "lone continuation byte", s: "\x80", want: []uint16{0xfffd}},
		{name: "overlong 2-byte", s: "\xc0\x80", want: []uint16{0xfffd, 0xfffd}},
		{name: "truncated 3-byte at end", s: "\xe2\x82", want: []uint16{0xfffd}},
		{name: "truncated 3-byte", s: "\xe2\x82A", want: []uint16{0xfffd, 0x41}},
		{name: "encoded surrogate", s: "\xed\xa0\x80", want: []uint16{0xfffd}},
		{name: "truncated encoded surrogate", s: "\xed\xa0A", want: []uint16{0xfffd, 0x41}},
		{name: "overlong 3-byte", s: "\xe0\x80\x80", want: []uint16{0xfffd, 0xfffd, 0xfffd}},
		{name: "truncated overlong 3-byte at end", s: "\xe0\x9f", want: []uint16{0xfffd, 0xfffd}},
		{name: "truncated encoded surrogate at end", s: "\xed\xa0", want: []uint16{0xfffd}},
		{name: "truncated 4-byte", s: "\xf0\x9f\x94A", want: []uint16{0xfffd, 0x41}},
		{name: "truncated 4-byte at end", s: "\xf0\x9f\xbf", want: []uint16{0xfffd}},
		{name: "truncated above U+10FFFF at end", s: "\xf4\x90", want: []uint16{0xfffd, 0xfffd}},
		{name: "above U+10FFFF", s: "\xf4\x90\x80\x80", want: []uint16{0xfffd, 0xfffd, 0xfffd, 0xfffd}},
		{name: "5-byte form", s: "\xf8\x88\x80\x80\x80", want: []uint16{0xfffd, 0xfffd, 0xfffd, 0xfffd, 0xfffd}},
		{name: "between valid characters", s: "a\xe2\x82b", want: []uint16{0x61, 0xfffd, 0x62}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var want []byte
			for _, unit := range tc.want {
				want = binary.LittleEndian.AppendUint16(want, unit)
			}
			hasher := xxhash.NewWithSeed(stringSliceHashSeed)
			hasher.Write(want) //nolint:errcheck

			assert.Equal(t, hasher.Sum64(), GenerateHashKeyFromStrings([]string{tc.s}))
		})
	}
}

func TestGenerateHashKeyFromStrings_JoinCollisions(t *testing.T) {
	// Elements are joined with "," without escaping, as in Java.
	assert.Equal(t, GenerateHashKeyFromStrings([]string{"a,b"}), GenerateHashKeyFromStrings([]string{"a", "b"}))
	assert.Equal(t, GenerateHashKeyFromStrings([]string{}), GenerateHashKeyFromStrings([]string{""}))
	assert.NotEqual(t, GenerateHashKeyFromStrings([]string{"a", "b"}), GenerateHashKeyFromStrings([]string{"b", "a"}))
}
