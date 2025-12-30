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

package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitArrayBasicOperations(t *testing.T) {
	// Create array with 128 bits (2 longs)
	array := make([]uint64, 2)

	// Test initial state - all bits should be 0
	assert.Equal(t, uint64(0), countBitsSet(array))
	assert.False(t, getBit(array, 0))
	assert.False(t, getBit(array, 63))
	assert.False(t, getBit(array, 64))
	assert.False(t, getBit(array, 127))

	// Test setBit
	setBit(array, 5)
	assert.True(t, getBit(array, 5))
	assert.Equal(t, uint64(1), countBitsSet(array))

	setBit(array, 65)
	assert.True(t, getBit(array, 65))
	assert.Equal(t, uint64(2), countBitsSet(array))

	// Test clearBit
	clearBit(array, 5)
	assert.False(t, getBit(array, 5))
	assert.Equal(t, uint64(1), countBitsSet(array))

	// Test assignBit
	assignBit(array, 10, true)
	assert.True(t, getBit(array, 10))
	assert.Equal(t, uint64(2), countBitsSet(array))

	assignBit(array, 10, false)
	assert.False(t, getBit(array, 10))
	assert.Equal(t, uint64(1), countBitsSet(array))

	// Test getAndSetBit
	wasSet := getAndSetBit(array, 20)
	assert.False(t, wasSet) // Was not set
	assert.True(t, getBit(array, 20))
	assert.Equal(t, uint64(2), countBitsSet(array))

	wasSet = getAndSetBit(array, 20)
	assert.True(t, wasSet) // Was already set
	assert.True(t, getBit(array, 20))
	assert.Equal(t, uint64(2), countBitsSet(array))
}

func TestBitArrayInversion(t *testing.T) {
	// Create array with 128 bits
	array := make([]uint64, 2)

	// Set some bits
	setBit(array, 0)
	setBit(array, 10)
	setBit(array, 63)
	setBit(array, 100)
	assert.Equal(t, uint64(4), countBitsSet(array))

	// Invert
	count := invert(array)
	assert.Equal(t, uint64(128-4), count)
	assert.Equal(t, uint64(128-4), countBitsSet(array))

	// Previously set bits should now be clear
	assert.False(t, getBit(array, 0))
	assert.False(t, getBit(array, 10))
	assert.False(t, getBit(array, 63))
	assert.False(t, getBit(array, 100))

	// Previously clear bits should now be set
	assert.True(t, getBit(array, 1))
	assert.True(t, getBit(array, 50))
	assert.True(t, getBit(array, 64))
	assert.True(t, getBit(array, 127))
}

func TestBitArrayUnion(t *testing.T) {
	// Create two arrays with 192 bits (3 longs)
	array1 := make([]uint64, 3)
	array2 := make([]uint64, 3)
	array3 := make([]uint64, 3)

	// Array1: bits 0-9
	for i := uint64(0); i < 10; i++ {
		setBit(array1, i)
	}

	// Array2: bits 5-14
	for i := uint64(5); i < 15; i++ {
		setBit(array2, i)
	}

	// Array3: even bits 0-18
	for i := uint64(0); i < 19; i += 2 {
		setBit(array3, i)
	}

	// Union of array2 and array3
	count := unionWith(array2, array3)
	// Array2 had bits 5-14 (10 bits)
	// Array3 had even bits 0-18 (10 bits: 0,2,4,6,8,10,12,14,16,18)
	// Union should have: 5,6,7,8,9,10,11,12,13,14 + 0,2,4,16,18 = 15 bits
	assert.Equal(t, uint64(15), count)
	assert.Equal(t, uint64(15), countBitsSet(array2))
}

func TestBitArrayIntersection(t *testing.T) {
	// Create two arrays
	array1 := make([]uint64, 3)
	array2 := make([]uint64, 3)

	// Array1: bits 0-9
	for i := uint64(0); i < 10; i++ {
		setBit(array1, i)
	}

	// Array2: bits 5-14
	for i := uint64(5); i < 15; i++ {
		setBit(array2, i)
	}

	// Intersection
	count := intersect(array1, array2)
	// Overlap is bits 5-9 = 5 bits
	assert.Equal(t, uint64(5), count)
	assert.Equal(t, uint64(5), countBitsSet(array1))

	// Verify the overlap bits
	for i := uint64(5); i < 10; i++ {
		assert.True(t, getBit(array1, i))
	}
	// Verify non-overlap bits are cleared
	for i := uint64(0); i < 5; i++ {
		assert.False(t, getBit(array1, i))
	}
}

func TestBitArrayBoundaries(t *testing.T) {
	// Test bit operations at long boundaries
	array := make([]uint64, 3) // 192 bits

	// Test at boundary of first and second long (bit 63-64)
	setBit(array, 63)
	setBit(array, 64)
	assert.True(t, getBit(array, 63))
	assert.True(t, getBit(array, 64))

	clearBit(array, 63)
	assert.False(t, getBit(array, 63))
	assert.True(t, getBit(array, 64))

	// Test at boundary of second and third long (bit 127-128)
	setBit(array, 127)
	setBit(array, 128)
	assert.True(t, getBit(array, 127))
	assert.True(t, getBit(array, 128))

	// Test last bit
	setBit(array, 191)
	assert.True(t, getBit(array, 191))
	assert.Equal(t, uint64(4), countBitsSet(array))
}
