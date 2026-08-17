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

func TestItemSketchLongComparator(t *testing.T) {
	ascending := ItemSketchLongComparator(false)
	assert.True(t, ascending(math.MinInt64, math.MaxInt64))
	assert.False(t, ascending(math.MaxInt64, math.MinInt64))
	assert.False(t, ascending(1, 1))

	descending := ItemSketchLongComparator(true)
	assert.True(t, descending(math.MaxInt64, math.MinInt64))
	assert.False(t, descending(math.MinInt64, math.MaxInt64))
	assert.False(t, descending(1, 1))
}

func TestItemSketchLongHasher_Hash(t *testing.T) {
	hasher := ItemSketchLongHasher{}
	tests := []int64{0, 1, -1, math.MinInt64, math.MaxInt64}

	for _, item := range tests {
		var bytes [8]byte
		binary.LittleEndian.PutUint64(bytes[:], uint64(item))
		expected := murmur3.SeedSum64(defaultSerdeHashSeed, bytes[:])
		assert.Equal(t, expected, hasher.Hash(item))
	}
}

func TestItemSketchLongSerDe_SizeOf(t *testing.T) {
	serde := ItemSketchLongSerDe{}
	assert.Equal(t, 8, serde.SizeOf(0))
	assert.Equal(t, 8, serde.SizeOf(math.MinInt64))
}

func TestItemSketchLongSerDe_SizeOfMany(t *testing.T) {
	serde := ItemSketchLongSerDe{}
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

func TestItemSketchLongSerDe_SerializeOneToSlice(t *testing.T) {
	serde := ItemSketchLongSerDe{}
	tests := []struct {
		name     string
		item     int64
		expected []byte
	}{
		{name: "zero", item: 0, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "positive", item: 1, expected: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "negative", item: -2, expected: []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, serde.SerializeOneToSlice(tt.item))
		})
	}
}

func TestItemSketchLongSerDe_SerializeManyToSlice(t *testing.T) {
	serde := ItemSketchLongSerDe{}
	tests := []struct {
		name  string
		items []int64
	}{
		{name: "nil slice", items: nil},
		{name: "empty slice", items: []int64{}},
		{name: "single item", items: []int64{1}},
		{name: "multiple items", items: []int64{1, -2, math.MinInt64}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := make([]byte, 8*len(tt.items))
			for i, item := range tt.items {
				binary.LittleEndian.PutUint64(expected[i*8:], uint64(item))
			}

			assert.Equal(t, expected, serde.SerializeManyToSlice(tt.items))
		})
	}
}

func TestItemSketchLongSerDe_DeserializeManyFromSlice(t *testing.T) {
	serde := ItemSketchLongSerDe{}
	tests := []struct {
		name        string
		expected    []int64
		offsetBytes int
	}{
		{name: "zero items", expected: []int64{}},
		{name: "single item", expected: []int64{1}},
		{name: "multiple items", expected: []int64{0, 1, -1}},
		{
			name:        "boundary values with offset",
			expected:    []int64{math.MinInt64, math.MaxInt64},
			offsetBytes: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := make([]byte, tt.offsetBytes+8*len(tt.expected))
			for i, item := range tt.expected {
				offset := tt.offsetBytes + i*8
				binary.LittleEndian.PutUint64(mem[offset:], uint64(item))
			}

			actual, err := serde.DeserializeManyFromSlice(mem, tt.offsetBytes, len(tt.expected))
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
