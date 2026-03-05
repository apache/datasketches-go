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

func TestItemSketchStringSerDe_SizeOf(t *testing.T) {
	serde := ItemSketchStringSerDe{}

	tests := []struct {
		name     string
		item     string
		expected int
	}{
		{
			name:     "empty string",
			item:     "",
			expected: 4,
		},
		{
			name:     "single character",
			item:     "a",
			expected: 5,
		},
		{
			name:     "ascii string",
			item:     "hello",
			expected: 9,
		},
		{
			name:     "multi-byte utf8 string",
			item:     "안녕하세요",
			expected: 19, // 15 bytes UTF-8 + 4
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := serde.SizeOf(tt.item)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestItemSketchStringSerDe_SizeOfMany(t *testing.T) {
	serde := ItemSketchStringSerDe{}

	tests := []struct {
		name        string
		items       []string
		offsetBytes int
		numItems    int
		expected    int
		expectedErr string
	}{
		{
			name:        "zero items",
			items:       nil,
			offsetBytes: 0,
			numItems:    0,
			expected:    0,
		},
		{
			name:        "negative items",
			items:       nil,
			offsetBytes: 0,
			numItems:    -1,
			expected:    0,
		},
		{
			name:        "single item",
			items:       []string{"hello"},
			offsetBytes: 0,
			numItems:    1,
			expected:    9,
		},
		{
			name:        "multiple items",
			items:       []string{"abc", "de"},
			offsetBytes: 0,
			numItems:    2,
			expected:    13, // (4+3) + (4+2)
		},
		{
			name:        "with offset",
			items:       []string{"hi"},
			offsetBytes: 2,
			numItems:    1,
			expected:    6, // 4+2
		},
		{
			name:        "offset out of bounds for length prefix",
			items:       nil,
			offsetBytes: 0,
			numItems:    1,
			expected:    0,
			expectedErr: "offset out of bounds",
		},
		{
			name:        "offset out of bounds for item data",
			items:       nil,
			offsetBytes: 0,
			numItems:    1,
			expected:    0,
			expectedErr: "offset out of bounds",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mem []byte
			switch tt.name {
			case "offset out of bounds for length prefix":
				mem = []byte{0x01, 0x02} // too short to read uint32
			case "offset out of bounds for item data":
				mem = make([]byte, 4)
				binary.LittleEndian.PutUint32(mem, 100) // claims 100 bytes but mem is only 4
			default:
				if tt.items != nil {
					mem = serde.SerializeManyToSlice(tt.items)
					if tt.offsetBytes > 0 {
						padded := make([]byte, tt.offsetBytes+len(mem))
						copy(padded[tt.offsetBytes:], mem)
						mem = padded
					}
				}
			}

			result, err := serde.SizeOfMany(mem, tt.offsetBytes, tt.numItems)
			if tt.expectedErr != "" {
				assert.EqualError(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestItemSketchStringSerDe_SerializeOneToSlice(t *testing.T) {
	serde := ItemSketchStringSerDe{}

	tests := []struct {
		name     string
		item     string
		expected []byte
	}{
		{
			name:     "empty string",
			item:     "",
			expected: []byte{},
		},
		{
			name: "single character",
			item: "a",
			expected: func() []byte {
				b := make([]byte, 5)
				binary.LittleEndian.PutUint32(b, 1)
				b[4] = 'a'
				return b
			}(),
		},
		{
			name: "ascii string",
			item: "hello",
			expected: func() []byte {
				b := make([]byte, 9)
				binary.LittleEndian.PutUint32(b, 5)
				copy(b[4:], "hello")
				return b
			}(),
		},
		{
			name: "multi-byte utf8 string",
			item: "안녕하세요",
			expected: func() []byte {
				utf8Bytes := []byte("안녕하세요")
				b := make([]byte, 4+len(utf8Bytes))
				binary.LittleEndian.PutUint32(b, uint32(len(utf8Bytes)))
				copy(b[4:], utf8Bytes)
				return b
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := serde.SerializeOneToSlice(tt.item)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestItemSketchStringSerDe_SerializeManyToSlice(t *testing.T) {
	serde := ItemSketchStringSerDe{}

	tests := []struct {
		name     string
		items    []string
		expected []byte
	}{
		{
			name:     "empty slice",
			items:    []string{},
			expected: []byte{},
		},
		{
			name:  "single item",
			items: []string{"abc"},
			expected: func() []byte {
				b := make([]byte, 7)
				binary.LittleEndian.PutUint32(b, 3)
				copy(b[4:], "abc")
				return b
			}(),
		},
		{
			name:  "multiple items",
			items: []string{"ab", "cde"},
			expected: func() []byte {
				b := make([]byte, 13) // (4+2) + (4+3)
				binary.LittleEndian.PutUint32(b[0:], 2)
				copy(b[4:], "ab")
				binary.LittleEndian.PutUint32(b[6:], 3)
				copy(b[10:], "cde")
				return b
			}(),
		},
		{
			name:  "items with empty string",
			items: []string{"a", "", "b"},
			expected: func() []byte {
				b := make([]byte, 14) // (4+1) + (4+0) + (4+1)
				binary.LittleEndian.PutUint32(b[0:], 1)
				b[4] = 'a'
				binary.LittleEndian.PutUint32(b[5:], 0)
				binary.LittleEndian.PutUint32(b[9:], 1)
				b[13] = 'b'
				return b
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := serde.SerializeManyToSlice(tt.items)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestItemSketchStringSerDe_DeserializeManyFromSlice(t *testing.T) {
	serde := ItemSketchStringSerDe{}

	tests := []struct {
		name        string
		mem         []byte
		offsetBytes int
		numItems    int
		expected    []string
		expectedErr string
	}{
		{
			name:        "zero items",
			mem:         nil,
			offsetBytes: 0,
			numItems:    0,
			expected:    []string{},
		},
		{
			name:        "negative items",
			mem:         nil,
			offsetBytes: 0,
			numItems:    -1,
			expected:    []string{},
		},
		{
			name:        "single item",
			mem:         serde.SerializeManyToSlice([]string{"hello"}),
			offsetBytes: 0,
			numItems:    1,
			expected:    []string{"hello"},
		},
		{
			name:        "multiple items",
			mem:         serde.SerializeManyToSlice([]string{"foo", "bar", "baz"}),
			offsetBytes: 0,
			numItems:    3,
			expected:    []string{"foo", "bar", "baz"},
		},
		{
			name: "with offset",
			mem: func() []byte {
				serialized := serde.SerializeManyToSlice([]string{"test"})
				padded := make([]byte, 5+len(serialized))
				copy(padded[5:], serialized)
				return padded
			}(),
			offsetBytes: 5,
			numItems:    1,
			expected:    []string{"test"},
		},
		{
			name:        "offset out of bounds for length prefix",
			mem:         []byte{0x01},
			offsetBytes: 0,
			numItems:    1,
			expected:    nil,
			expectedErr: "offset out of bounds",
		},
		{
			name: "offset out of bounds for item data",
			mem: func() []byte {
				b := make([]byte, 4)
				binary.LittleEndian.PutUint32(b, 100)
				return b
			}(),
			offsetBytes: 0,
			numItems:    1,
			expected:    nil,
			expectedErr: "offset out of bounds",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serde.DeserializeManyFromSlice(tt.mem, tt.offsetBytes, tt.numItems)
			if tt.expectedErr != "" {
				assert.EqualError(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
