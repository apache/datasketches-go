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

// Package filters provides probabilistic membership data structures for efficient
// set membership testing with controlled false positive rates.
//
// The Bloom filter is a space-efficient probabilistic data structure that is used
// to test whether an element is a member of a set. False positive matches are
// possible, but false negatives are not. This implementation uses XXHash64 with
// Kirsch-Mitzenmacher double hashing optimization.
package filters

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/cespare/xxhash/v2"
)

// BloomFilter is a probabilistic data structure for set membership testing.
// It provides constant-time updates and queries with a configurable false
// positive rate. No false negatives are possible.
type BloomFilter interface {
	// Update methods add items to the filter
	UpdateUInt64(datum uint64) error
	UpdateInt64(datum int64) error
	UpdateString(datum string) error
	UpdateSlice(datum []byte) error
	UpdateFloat64(datum float64) error
	UpdateInt64Array(data []int64) error
	UpdateFloat64Array(data []float64) error

	// Query methods test membership
	QueryUInt64(datum uint64) bool
	QueryInt64(datum int64) bool
	QueryString(datum string) bool
	QuerySlice(datum []byte) bool
	QueryFloat64(datum float64) bool
	QueryInt64Array(data []int64) bool
	QueryFloat64Array(data []float64) bool

	// QueryAndUpdate atomically queries and updates (test-and-set)
	QueryAndUpdateUInt64(datum uint64) (bool, error)
	QueryAndUpdateInt64(datum int64) (bool, error)
	QueryAndUpdateString(datum string) (bool, error)
	QueryAndUpdateSlice(datum []byte) (bool, error)
	QueryAndUpdateFloat64(datum float64) (bool, error)
	QueryAndUpdateInt64Array(data []int64) (bool, error)
	QueryAndUpdateFloat64Array(data []float64) (bool, error)

	// Set operations
	Union(other BloomFilter) error
	Intersect(other BloomFilter) error
	Invert() error
	IsCompatible(other BloomFilter) bool

	// State queries
	IsEmpty() bool
	BitsUsed() uint64
	Capacity() uint64
	NumHashes() uint16
	Seed() uint64

	// Serialization
	ToCompactSlice() ([]byte, error)
	Reset() error
}

// bloomFilterImpl is the concrete implementation of BloomFilter.
type bloomFilterImpl struct {
	seed         uint64
	numHashes    uint16
	isDirty      bool
	capacityBits uint64
	numBitsSet   uint64
	bitArray     []uint64
}

// IsEmpty returns true if no bits are set in the filter.
func (bf *bloomFilterImpl) IsEmpty() bool {
	return bf.BitsUsed() == 0
}

// BitsUsed returns the number of bits currently set to 1.
// If the count is dirty, it will be recomputed.
func (bf *bloomFilterImpl) BitsUsed() uint64 {
	if bf.isDirty {
		bf.numBitsSet = countBitsSet(bf.bitArray)
		bf.isDirty = false
	}
	return bf.numBitsSet
}

// Capacity returns the total number of bits in the filter.
func (bf *bloomFilterImpl) Capacity() uint64 {
	return bf.capacityBits
}

// NumHashes returns the number of hash functions used.
func (bf *bloomFilterImpl) NumHashes() uint16 {
	return bf.numHashes
}

// Seed returns the hash seed used by the filter.
func (bf *bloomFilterImpl) Seed() uint64 {
	return bf.seed
}

// Reset clears all bits in the filter.
func (bf *bloomFilterImpl) Reset() error {
	for i := range bf.bitArray {
		bf.bitArray[i] = 0
	}
	bf.numBitsSet = 0
	bf.isDirty = false
	return nil
}

// IsCompatible checks if two filters can be combined (union/intersection).
// Filters are compatible if they have the same seed, hash count, and capacity.
func (bf *bloomFilterImpl) IsCompatible(other BloomFilter) bool {
	return bf.seed == other.Seed() &&
		bf.numHashes == other.NumHashes() &&
		bf.capacityBits == other.Capacity()
}

// computeHashes computes two hash values using XXHash64 and Kirsch-Mitzenmacher approach.
func (bf *bloomFilterImpl) computeHashes(data []byte) (h0, h1 uint64) {
	// Compute h0 with the filter's seed
	h := xxhash.NewWithSeed(bf.seed)
	h.Write(data)
	h0 = h.Sum64()

	// Compute h1 using h0 as seed
	h.Reset()
	h = xxhash.NewWithSeed(h0)
	h.Write(data)
	h1 = h.Sum64()
	return
}

// hashLongOptimized implements the Java-compatible optimized hash for single long values.
// This matches the implementation in org.apache.datasketches.hash.XxHash64.hash(long, long).
func hashLongOptimized(value uint64, seed uint64) uint64 {
	const (
		P1 = 0x9E3779B185EBCA87
		P2 = 0xC2B2AE3D27D4EB4F
		P3 = 0x165667B19E3779F9
		P4 = 0x85EBCA77C2B2AE63
		P5 = 0x27D4EB2F165667C5
	)

	hash := seed + P5
	hash += 8 // length in bytes

	k1 := value
	k1 *= P2
	k1 = bits.RotateLeft64(k1, 31)
	k1 *= P1
	hash ^= k1
	hash = (bits.RotateLeft64(hash, 27) * P1) + P4

	// Finalize
	hash ^= hash >> 33
	hash *= P2
	hash ^= hash >> 29
	hash *= P3
	hash ^= hash >> 32

	return hash
}

// computeHashesForLong computes hashes using the optimized single-long algorithm
// to match Java/C++ behavior for integer values.
func (bf *bloomFilterImpl) computeHashesForLong(value uint64) (h0, h1 uint64) {
	h0 = hashLongOptimized(value, bf.seed)
	h1 = hashLongOptimized(value, h0)
	return
}

// getHashIndex computes the i-th hash index using double hashing.
// Formula: g_i(x) = ((h0 + i * h1) >> 1) mod capacity
func (bf *bloomFilterImpl) getHashIndex(h0, h1 uint64, i uint16) uint64 {
	return ((h0 + uint64(i)*h1) >> 1) % bf.capacityBits
}

// internalUpdate updates the filter with pre-computed hash values.
func (bf *bloomFilterImpl) internalUpdate(h0, h1 uint64) {
	for i := uint16(1); i <= bf.numHashes; i++ {
		idx := bf.getHashIndex(h0, h1, i)
		if !getBit(bf.bitArray, idx) {
			setBit(bf.bitArray, idx)
			bf.numBitsSet++
		}
	}
	bf.isDirty = true
}

// internalQuery queries the filter with pre-computed hash values.
func (bf *bloomFilterImpl) internalQuery(h0, h1 uint64) bool {
	if bf.IsEmpty() {
		return false
	}
	for i := uint16(1); i <= bf.numHashes; i++ {
		idx := bf.getHashIndex(h0, h1, i)
		if !getBit(bf.bitArray, idx) {
			return false
		}
	}
	return true
}

// internalQueryAndUpdate atomically queries and updates with pre-computed hash values.
// Returns true if the item was already present (all k bits were set).
func (bf *bloomFilterImpl) internalQueryAndUpdate(h0, h1 uint64) bool {
	valueExists := true
	newBitsSet := uint64(0)
	for i := uint16(1); i <= bf.numHashes; i++ {
		idx := bf.getHashIndex(h0, h1, i)
		wasSet := getAndSetBit(bf.bitArray, idx)
		valueExists = valueExists && wasSet
		if !wasSet {
			newBitsSet++
		}
	}
	bf.numBitsSet += newBitsSet
	bf.isDirty = true
	return valueExists
}

// UpdateUInt64 adds a uint64 value to the filter.
func (bf *bloomFilterImpl) UpdateUInt64(datum uint64) error {
	h0, h1 := bf.computeHashesForLong(datum)
	bf.internalUpdate(h0, h1)
	return nil
}

// UpdateInt64 adds an int64 value to the filter.
func (bf *bloomFilterImpl) UpdateInt64(datum int64) error {
	h0, h1 := bf.computeHashesForLong(uint64(datum))
	bf.internalUpdate(h0, h1)
	return nil
}

// UpdateString adds a string to the filter.
// Empty strings are ignored (no update performed).
func (bf *bloomFilterImpl) UpdateString(datum string) error {
	if datum == "" {
		return nil // Empty string - do nothing, matching Java behavior
	}
	return bf.UpdateSlice([]byte(datum))
}

// UpdateSlice adds a byte slice to the filter.
func (bf *bloomFilterImpl) UpdateSlice(datum []byte) error {
	h0, h1 := bf.computeHashes(datum)
	bf.internalUpdate(h0, h1)
	return nil
}

// UpdateFloat64 adds a float64 value to the filter.
// NaN values are canonicalized to match Java's Double.doubleToLongBits().
func (bf *bloomFilterImpl) UpdateFloat64(datum float64) error {
	var bits uint64
	if datum == 0.0 {
		// Canonicalize -0.0 to 0.0
		bits = 0
	} else if math.IsNaN(datum) {
		// Use Java's canonical NaN: 0x7ff8000000000000
		bits = 0x7ff8000000000000
	} else if math.IsInf(datum, 1) {
		// Positive infinity
		bits = 0x7ff0000000000000
	} else if math.IsInf(datum, -1) {
		// Negative infinity
		bits = 0xfff0000000000000
	} else {
		bits = math.Float64bits(datum)
	}

	// Java hashes as a single-element long array (8 bytes), not using optimized single-long hash
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, bits)
	h0, h1 := bf.computeHashes(buf)
	bf.internalUpdate(h0, h1)
	return nil
}

// QueryUInt64 tests if a uint64 value might be in the filter.
func (bf *bloomFilterImpl) QueryUInt64(datum uint64) bool {
	h0, h1 := bf.computeHashesForLong(datum)
	return bf.internalQuery(h0, h1)
}

// QueryInt64 tests if an int64 value might be in the filter.
func (bf *bloomFilterImpl) QueryInt64(datum int64) bool {
	h0, h1 := bf.computeHashesForLong(uint64(datum))
	return bf.internalQuery(h0, h1)
}

// QueryString tests if a string might be in the filter.
// Empty strings always return false.
func (bf *bloomFilterImpl) QueryString(datum string) bool {
	if datum == "" {
		return false // Empty string - do nothing, matching Java behavior
	}
	return bf.QuerySlice([]byte(datum))
}

// QuerySlice tests if a byte slice might be in the filter.
func (bf *bloomFilterImpl) QuerySlice(datum []byte) bool {
	h0, h1 := bf.computeHashes(datum)
	return bf.internalQuery(h0, h1)
}

// QueryFloat64 tests if a float64 value might be in the filter.
func (bf *bloomFilterImpl) QueryFloat64(datum float64) bool {
	var bits uint64
	if datum == 0.0 {
		// Canonicalize -0.0 to 0.0
		bits = 0
	} else if math.IsNaN(datum) {
		// Use Java's canonical NaN: 0x7ff8000000000000
		bits = 0x7ff8000000000000
	} else if math.IsInf(datum, 1) {
		// Positive infinity
		bits = 0x7ff0000000000000
	} else if math.IsInf(datum, -1) {
		// Negative infinity
		bits = 0xfff0000000000000
	} else {
		bits = math.Float64bits(datum)
	}

	// Java hashes as a single-element long array (8 bytes), not using optimized single-long hash
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, bits)
	h0, h1 := bf.computeHashes(buf)
	return bf.internalQuery(h0, h1)
}

// QueryAndUpdateUInt64 atomically queries and updates for a uint64 value.
// Returns true if the value was already present before the update.
func (bf *bloomFilterImpl) QueryAndUpdateUInt64(datum uint64) (bool, error) {
	h0, h1 := bf.computeHashesForLong(datum)
	return bf.internalQueryAndUpdate(h0, h1), nil
}

// QueryAndUpdateInt64 atomically queries and updates for an int64 value.
func (bf *bloomFilterImpl) QueryAndUpdateInt64(datum int64) (bool, error) {
	h0, h1 := bf.computeHashesForLong(uint64(datum))
	return bf.internalQueryAndUpdate(h0, h1), nil
}

// QueryAndUpdateString atomically queries and updates for a string.
// Empty strings are ignored and return false.
func (bf *bloomFilterImpl) QueryAndUpdateString(datum string) (bool, error) {
	if datum == "" {
		return false, nil // Empty string - do nothing, matching Java behavior
	}
	return bf.QueryAndUpdateSlice([]byte(datum))
}

// QueryAndUpdateSlice atomically queries and updates for a byte slice.
func (bf *bloomFilterImpl) QueryAndUpdateSlice(datum []byte) (bool, error) {
	h0, h1 := bf.computeHashes(datum)
	return bf.internalQueryAndUpdate(h0, h1), nil
}

// QueryAndUpdateFloat64 atomically queries and updates for a float64 value.
func (bf *bloomFilterImpl) QueryAndUpdateFloat64(datum float64) (bool, error) {
	var bits uint64
	if datum == 0.0 {
		// Canonicalize -0.0 to 0.0
		bits = 0
	} else if math.IsNaN(datum) {
		// Use Java's canonical NaN: 0x7ff8000000000000
		bits = 0x7ff8000000000000
	} else if math.IsInf(datum, 1) {
		// Positive infinity
		bits = 0x7ff0000000000000
	} else if math.IsInf(datum, -1) {
		// Negative infinity
		bits = 0xfff0000000000000
	} else {
		bits = math.Float64bits(datum)
	}

	// Java hashes as a single-element long array (8 bytes), not using optimized single-long hash
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, bits)
	h0, h1 := bf.computeHashes(buf)
	return bf.internalQueryAndUpdate(h0, h1), nil
}

// UpdateInt64Array adds an array of int64 values to the filter.
// The entire array is hashed as a single unit (not element-by-element).
// Nil or empty arrays are ignored.
func (bf *bloomFilterImpl) UpdateInt64Array(data []int64) error {
	if len(data) == 0 {
		return nil
	}

	// Convert array to bytes (little-endian)
	bytes := make([]byte, len(data)*8)
	for i, val := range data {
		binary.LittleEndian.PutUint64(bytes[i*8:], uint64(val))
	}

	h0, h1 := bf.computeHashes(bytes)
	bf.internalUpdate(h0, h1)
	return nil
}

// UpdateFloat64Array adds an array of float64 values to the filter.
// The entire array is hashed as a single unit (not element-by-element).
// Nil or empty arrays are ignored.
func (bf *bloomFilterImpl) UpdateFloat64Array(data []float64) error {
	if len(data) == 0 {
		return nil
	}

	// Convert array to bytes (little-endian)
	bytes := make([]byte, len(data)*8)
	for i, val := range data {
		bits := math.Float64bits(val)
		binary.LittleEndian.PutUint64(bytes[i*8:], bits)
	}

	h0, h1 := bf.computeHashes(bytes)
	bf.internalUpdate(h0, h1)
	return nil
}

// QueryInt64Array tests if an array of int64 values might be in the filter.
// The entire array is hashed as a single unit.
func (bf *bloomFilterImpl) QueryInt64Array(data []int64) bool {
	if len(data) == 0 {
		return false
	}

	// Convert array to bytes (little-endian)
	bytes := make([]byte, len(data)*8)
	for i, val := range data {
		binary.LittleEndian.PutUint64(bytes[i*8:], uint64(val))
	}

	h0, h1 := bf.computeHashes(bytes)
	return bf.internalQuery(h0, h1)
}

// QueryFloat64Array tests if an array of float64 values might be in the filter.
// The entire array is hashed as a single unit.
func (bf *bloomFilterImpl) QueryFloat64Array(data []float64) bool {
	if len(data) == 0 {
		return false
	}

	// Convert array to bytes (little-endian)
	bytes := make([]byte, len(data)*8)
	for i, val := range data {
		bits := math.Float64bits(val)
		binary.LittleEndian.PutUint64(bytes[i*8:], bits)
	}

	h0, h1 := bf.computeHashes(bytes)
	return bf.internalQuery(h0, h1)
}

// QueryAndUpdateInt64Array atomically queries and updates for an int64 array.
// Returns true if the array was already present before the update.
func (bf *bloomFilterImpl) QueryAndUpdateInt64Array(data []int64) (bool, error) {
	if len(data) == 0 {
		return false, nil
	}

	// Convert array to bytes (little-endian)
	bytes := make([]byte, len(data)*8)
	for i, val := range data {
		binary.LittleEndian.PutUint64(bytes[i*8:], uint64(val))
	}

	h0, h1 := bf.computeHashes(bytes)
	return bf.internalQueryAndUpdate(h0, h1), nil
}

// QueryAndUpdateFloat64Array atomically queries and updates for a float64 array.
// Returns true if the array was already present before the update.
func (bf *bloomFilterImpl) QueryAndUpdateFloat64Array(data []float64) (bool, error) {
	if len(data) == 0 {
		return false, nil
	}

	// Convert array to bytes (little-endian)
	bytes := make([]byte, len(data)*8)
	for i, val := range data {
		bits := math.Float64bits(val)
		binary.LittleEndian.PutUint64(bytes[i*8:], bits)
	}

	h0, h1 := bf.computeHashes(bytes)
	return bf.internalQueryAndUpdate(h0, h1), nil
}

// Union performs a bitwise OR operation with another filter.
// After union, this filter will contain items from both filters.
func (bf *bloomFilterImpl) Union(other BloomFilter) error {
	if !bf.IsCompatible(other) {
		return fmt.Errorf("cannot union incompatible bloom filters")
	}
	otherImpl, ok := other.(*bloomFilterImpl)
	if !ok {
		return fmt.Errorf("cannot union with non-standard bloom filter implementation")
	}
	bf.numBitsSet = unionWith(bf.bitArray, otherImpl.bitArray)
	bf.isDirty = false
	return nil
}

// Intersect performs a bitwise AND operation with another filter.
// After intersection, this filter will only contain items present in both filters.
func (bf *bloomFilterImpl) Intersect(other BloomFilter) error {
	if !bf.IsCompatible(other) {
		return fmt.Errorf("cannot intersect incompatible bloom filters")
	}
	otherImpl, ok := other.(*bloomFilterImpl)
	if !ok {
		return fmt.Errorf("cannot intersect with non-standard bloom filter implementation")
	}
	bf.numBitsSet = intersect(bf.bitArray, otherImpl.bitArray)
	bf.isDirty = false
	return nil
}

// Invert flips all bits in the filter.
// This inverts the notion of membership.
func (bf *bloomFilterImpl) Invert() error {
	bf.numBitsSet = invert(bf.bitArray)
	bf.isDirty = false
	return nil
}

// ToCompactSlice serializes the filter to a byte slice.
func (bf *bloomFilterImpl) ToCompactSlice() ([]byte, error) {
	isEmpty := bf.IsEmpty()
	var size int
	if isEmpty {
		size = preambleEmptyBytes
	} else {
		size = preambleBytes + len(bf.bitArray)*8
	}

	bytes := make([]byte, size)

	// Write preamble
	if isEmpty {
		insertPreambleLongs(bytes, preambleLongsEmpty)
	} else {
		insertPreambleLongs(bytes, preambleLongsStandard)
	}
	insertSerVer(bytes)
	insertFamilyID(bytes)

	flags := uint8(0)
	if isEmpty {
		flags = setEmptyFlag(flags)
	}
	insertFlags(bytes, flags)
	insertNumHashes(bytes, bf.numHashes)
	insertSeed(bytes, bf.seed)
	insertBitArrayLength(bytes, uint32(len(bf.bitArray)))

	if !isEmpty {
		bitsUsed := bf.BitsUsed()
		insertNumBitsSet(bytes, bitsUsed)

		// Write bit array
		for i, val := range bf.bitArray {
			binary.LittleEndian.PutUint64(bytes[bitArrayOffset+i*8:], val)
		}
	}

	return bytes, nil
}
