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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/murmur3"
)

func TestItemSketchDoubleComparator(t *testing.T) {
	ascending := ItemSketchDoubleComparator(false)
	assert.True(t, ascending(-1.5, 2.5))
	assert.False(t, ascending(2.5, -1.5))
	assert.False(t, ascending(1.0, 1.0))

	descending := ItemSketchDoubleComparator(true)
	assert.True(t, descending(2.5, -1.5))
	assert.False(t, descending(-1.5, 2.5))
	assert.False(t, descending(1.0, 1.0))
}

func TestItemSketchDoubleHasher_Hash(t *testing.T) {
	hasher := ItemSketchDoubleHasher{}
	tests := []float64{
		0,
		math.Copysign(0, -1),
		1.5,
		-2.25,
		math.Inf(1),
		math.Float64frombits(0x7ff8000000001234),
	}

	for _, item := range tests {
		var bytes [8]byte
		binary.LittleEndian.PutUint64(bytes[:], math.Float64bits(item))
		expected := murmur3.SeedSum64(defaultSerdeHashSeed, bytes[:])
		assert.Equal(t, expected, hasher.Hash(item))
	}
}

func TestItemSketchDoubleSerDe_SizeOf(t *testing.T) {
	serde := ItemSketchDoubleSerDe{}
	assert.Equal(t, 8, serde.SizeOf(0))
	assert.Equal(t, 8, serde.SizeOf(math.Inf(1)))
}

func TestItemSketchDoubleSerDe_SizeOfMany(t *testing.T) {
	serde := ItemSketchDoubleSerDe{}
	tests := []struct {
		name     string
		numItems int
		expected int
	}{
		{name: "zero items", numItems: 0, expected: 0},
		{name: "one item", numItems: 1, expected: 8},
		{name: "multiple items", numItems: 3, expected: 24},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serde.SizeOfMany(nil, 0, tt.numItems)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestItemSketchDoubleSerDe_SerializeOneToSlice(t *testing.T) {
	serde := ItemSketchDoubleSerDe{}
	tests := []struct {
		name     string
		item     float64
		expected []byte
	}{
		{name: "zero", item: 0, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "positive", item: 1.5, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f}},
		{name: "negative", item: -2.25, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xc0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, serde.SerializeOneToSlice(tt.item))
		})
	}
}

func TestItemSketchDoubleSerDe_SerializeManyToSlice(t *testing.T) {
	serde := ItemSketchDoubleSerDe{}
	tests := []struct {
		name  string
		items []float64
	}{
		{name: "nil slice", items: nil},
		{name: "empty slice", items: []float64{}},
		{name: "single item", items: []float64{1.5}},
		{name: "multiple items", items: []float64{1.5, -2.25, math.Inf(1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := make([]byte, 8*len(tt.items))
			for i, item := range tt.items {
				binary.LittleEndian.PutUint64(expected[i*8:], math.Float64bits(item))
			}

			assert.Equal(t, expected, serde.SerializeManyToSlice(tt.items))
		})
	}
}

func TestItemSketchDoubleSerDe_DeserializeManyFromSlice(t *testing.T) {
	serde := ItemSketchDoubleSerDe{}
	tests := []struct {
		name        string
		expected    []float64
		offsetBytes int
	}{
		{name: "zero items", expected: []float64{}},
		{name: "single item", expected: []float64{1.5}},
		{name: "multiple items", expected: []float64{1.5, -2.25, math.Inf(1)}},
		{
			name: "special values with offset",
			expected: []float64{
				math.Copysign(0, -1),
				math.Float64frombits(0x7ff8000000001234),
				math.Inf(-1),
			},
			offsetBytes: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := make([]byte, tt.offsetBytes+8*len(tt.expected))
			for i, item := range tt.expected {
				offset := tt.offsetBytes + i*8
				binary.LittleEndian.PutUint64(mem[offset:], math.Float64bits(item))
			}

			actual, err := serde.DeserializeManyFromSlice(mem, tt.offsetBytes, len(tt.expected))
			assert.NoError(t, err)
			if assert.Len(t, actual, len(tt.expected)) {
				for i := range tt.expected {
					assert.Equal(t, math.Float64bits(tt.expected[i]), math.Float64bits(actual[i]))
				}
			}
		})
	}
}
