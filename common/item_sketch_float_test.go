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
)

func TestItemSketchFloatComparator(t *testing.T) {
	ascending := ItemSketchFloatComparator(false)
	assert.True(t, ascending(float32(-1.5), float32(2.5)))
	assert.False(t, ascending(float32(2.5), float32(-1.5)))
	assert.False(t, ascending(float32(1), float32(1)))

	descending := ItemSketchFloatComparator(true)
	assert.True(t, descending(float32(2.5), float32(-1.5)))
	assert.False(t, descending(float32(-1.5), float32(2.5)))
	assert.False(t, descending(float32(1), float32(1)))
}

func TestItemSketchFloatSerDe_SizeOf(t *testing.T) {
	serde := ItemSketchFloatSerDe{}
	assert.Equal(t, 4, serde.SizeOf(0))
	assert.Equal(t, 4, serde.SizeOf(float32(math.Inf(1))))
}

func TestItemSketchFloatSerDe_SizeOfMany(t *testing.T) {
	serde := ItemSketchFloatSerDe{}
	tests := []struct {
		name     string
		numItems int
		expected int
	}{
		{name: "zero items", numItems: 0, expected: 0},
		{name: "one item", numItems: 1, expected: 4},
		{name: "multiple items", numItems: 3, expected: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serde.SizeOfMany(nil, 0, tt.numItems)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestItemSketchFloatSerDe_SerializeOneToSlice(t *testing.T) {
	serde := ItemSketchFloatSerDe{}
	tests := []struct {
		name     string
		item     float32
		expected []byte
	}{
		{name: "zero", item: 0, expected: []byte{0x00, 0x00, 0x00, 0x00}},
		{name: "positive", item: 1.5, expected: []byte{0x00, 0x00, 0xc0, 0x3f}},
		{name: "negative", item: -2.25, expected: []byte{0x00, 0x00, 0x10, 0xc0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, serde.SerializeOneToSlice(tt.item))
		})
	}
}

func TestItemSketchFloatSerDe_SerializeManyToSlice(t *testing.T) {
	serde := ItemSketchFloatSerDe{}
	tests := []struct {
		name  string
		items []float32
	}{
		{name: "nil slice", items: nil},
		{name: "empty slice", items: []float32{}},
		{name: "single item", items: []float32{1.5}},
		{name: "multiple items", items: []float32{1.5, -2.25, float32(math.Inf(1))}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := make([]byte, 4*len(tt.items))
			for i, item := range tt.items {
				binary.LittleEndian.PutUint32(expected[i*4:], math.Float32bits(item))
			}

			assert.Equal(t, expected, serde.SerializeManyToSlice(tt.items))
		})
	}
}

func TestItemSketchFloatSerDe_DeserializeManyFromSlice(t *testing.T) {
	serde := ItemSketchFloatSerDe{}
	tests := []struct {
		name        string
		expected    []float32
		offsetBytes int
	}{
		{name: "zero items", expected: []float32{}},
		{name: "single item", expected: []float32{1.5}},
		{name: "multiple items", expected: []float32{1.5, -2.25, float32(math.Inf(1))}},
		{
			name: "special values with offset",
			expected: []float32{
				math.Float32frombits(0x80000000),
				math.Float32frombits(0x7fc01234),
				float32(math.Inf(-1)),
			},
			offsetBytes: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := make([]byte, tt.offsetBytes+4*len(tt.expected))
			for i, item := range tt.expected {
				offset := tt.offsetBytes + i*4
				binary.LittleEndian.PutUint32(mem[offset:], math.Float32bits(item))
			}

			actual, err := serde.DeserializeManyFromSlice(mem, tt.offsetBytes, len(tt.expected))
			assert.NoError(t, err)
			if assert.Len(t, actual, len(tt.expected)) {
				for i := range tt.expected {
					assert.Equal(t, math.Float32bits(tt.expected[i]), math.Float32bits(actual[i]))
				}
			}
		})
	}
}
