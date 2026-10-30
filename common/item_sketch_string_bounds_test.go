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

package common

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringSerDeRejectsMalformedInput(t *testing.T) {
	serde := ItemSketchStringSerDe{}
	maxInt := int(^uint(0) >> 1)
	prefix := func(length uint32) []byte {
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, length)
		return data
	}
	for _, tc := range []struct {
		name          string
		data          []byte
		offset, count int
	}{
		{"negative offset", prefix(0), -1, 1},
		{"overflowing offset", prefix(0), maxInt, 1},
		{"impossible item count", prefix(0), 0, maxInt},
		{"missing second prefix", prefix(0), 0, 2},
		{"truncated length", []byte{1, 0, 0}, 0, 1},
		{"truncated payload", prefix(5), 0, 1},
		{"signed length boundary", prefix(1 << 31), 0, 1},
		{"maximum unsigned length", prefix(^uint32(0)), 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				size, err := serde.SizeOfMany(tc.data, tc.offset, tc.count)
				assert.Error(t, err)
				assert.Zero(t, size)
			})
			assert.NotPanics(t, func() {
				items, err := serde.DeserializeManyFromSlice(tc.data, tc.offset, tc.count)
				assert.Error(t, err)
				assert.Nil(t, items)
			})
		})
	}
}

func TestStringSerDePreservesOffsetAndOwnership(t *testing.T) {
	serde := ItemSketchStringSerDe{}
	expected := []string{"", "hello", "日本語", "😀", "a\x00b"}
	encoded := serde.SerializeManyToSlice(expected)
	data := append([]byte{0xff, 0xff, 0xff}, encoded...)
	data = append(data, 0xfe)
	size, err := serde.SizeOfMany(data, 3, len(expected))
	assert.NoError(t, err)
	assert.Equal(t, len(encoded), size)
	actual, err := serde.DeserializeManyFromSlice(data, 3, len(expected))
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
	for i := range data {
		data[i] = 0
	}
	assert.Equal(t, expected, actual)
}

func FuzzStringSerDeBounds(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0}, uint8(0), uint8(1))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}, uint8(0), uint8(1))
	f.Add([]byte{1, 0, 0, 0, 'a'}, uint8(0), uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, offset, count uint8) {
		serde := ItemSketchStringSerDe{}
		n := int(count % 9)
		size, sizeErr := serde.SizeOfMany(data, int(offset), n)
		items, decodeErr := serde.DeserializeManyFromSlice(data, int(offset), n)
		if (sizeErr == nil) != (decodeErr == nil) {
			t.Fatalf("size and decoding disagree: %v / %v", sizeErr, decodeErr)
		}
		if decodeErr != nil {
			return
		}
		if len(items) != n || size > len(data) {
			t.Fatalf("invalid successful result: %d items, %d bytes", len(items), size)
		}
		if n > 0 {
			encoded := serde.SerializeManyToSlice(items)
			if len(encoded) != size {
				t.Fatalf("round trip size: got %d, want %d", len(encoded), size)
			}
		}
	})
}
