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

func TestGenerateHashKeyFromStrings_JoinCollisions(t *testing.T) {
	// Elements are joined with "," without escaping, as in Java.
	assert.Equal(t, GenerateHashKeyFromStrings([]string{"a,b"}), GenerateHashKeyFromStrings([]string{"a", "b"}))
	assert.Equal(t, GenerateHashKeyFromStrings([]string{}), GenerateHashKeyFromStrings([]string{""}))
	assert.NotEqual(t, GenerateHashKeyFromStrings([]string{"a", "b"}), GenerateHashKeyFromStrings([]string{"b", "a"}))
}
