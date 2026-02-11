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
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/datasketches-go/internal"
)

// DefaultSeed is the default seed value for hash functions
const DefaultSeed = uint64(9001)

var maxBits = uint64((math.MaxInt32 - internal.FamilyEnum.BloomFilter.MaxPreLongs) * 64)

// bloomFilterOptions holds optional parameters for filter construction.
type bloomFilterOptions struct {
	seed uint64
}

// BloomFilterOption is a functional option for configuring a BloomFilter.
type BloomFilterOption func(*bloomFilterOptions)

// WithSeed sets a custom seed for the hash functions.
func WithSeed(seed uint64) BloomFilterOption {
	return func(opts *bloomFilterOptions) {
		opts.seed = seed
	}
}

// NewBloomFilterBySize creates a new Bloom filter with explicit size parameters.
//
// Parameters:
//   - numBits: The number of bits in the filter (will be rounded up to multiple of 64)
//   - numHashes: The number of hash functions to use
//   - opts: Optional configuration (seed)
//
// Returns an error if parameters are invalid.
func NewBloomFilterBySize(numBits uint64, numHashes uint16, opts ...BloomFilterOption) (BloomFilter, error) {
	if numBits == 0 {
		return nil, fmt.Errorf("numBits must be positive")
	}
	if numHashes == 0 {
		return nil, fmt.Errorf("numHashes must be positive")
	}

	// Check for overflow
	if numBits > maxBits {
		return nil, fmt.Errorf("numBits exceeds maximum allowed size")
	}

	// Apply options
	options := &bloomFilterOptions{
		seed: DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	// Round capacity to multiple of 64
	capacityBits := roundCapacity(numBits)
	numLongs := capacityBits / 64

	return &bloomFilterImpl{
		seed:         options.seed,
		numHashes:    numHashes,
		isDirty:      false,
		capacityBits: capacityBits,
		numBitsSet:   0,
		bitArray:     make([]uint64, numLongs),
	}, nil
}

// NewBloomFilterByAccuracy creates a new Bloom filter optimized for target accuracy.
//
// The filter is sized to achieve the specified false positive probability for the
// given number of expected items. The optimal number of bits and hash functions
// are calculated automatically.
//
// Parameters:
//   - maxDistinctItems: Expected number of distinct items to be inserted
//   - targetFpp: Target false positive probability (between 0 and 1)
//   - opts: Optional configuration (seed)
//
// Returns an error if parameters are invalid.
func NewBloomFilterByAccuracy(maxDistinctItems uint64, targetFpp float64, opts ...BloomFilterOption) (BloomFilter, error) {
	if maxDistinctItems == 0 {
		return nil, fmt.Errorf("maxDistinctItems must be positive")
	}
	if targetFpp <= 0.0 || targetFpp >= 1.0 {
		return nil, fmt.Errorf("targetFpp must be between 0 and 1")
	}

	// Calculate optimal parameters
	numBits := SuggestNumFilterBits(maxDistinctItems, targetFpp)
	numHashes := SuggestNumHashesFromSize(maxDistinctItems, numBits)

	return NewBloomFilterBySize(numBits, numHashes, opts...)
}

// NewBloomFilterWithDefault creates a new Bloom filter with default parameters.
// Suitable for approximately 10,000 items with 1% false positive rate.
func NewBloomFilterWithDefault() (BloomFilter, error) {
	return NewBloomFilterByAccuracy(10000, 0.01)
}

// SuggestNumFilterBits calculates the optimal number of bits for a Bloom filter.
//
// Formula: m = ceil(-n * ln(p) / (ln(2))^2)
// where n = number of items, p = target false positive probability
func SuggestNumFilterBits(maxDistinctItems uint64, targetFpp float64) uint64 {
	n := float64(maxDistinctItems)
	p := targetFpp
	ln2 := math.Ln2

	bits := -n * math.Log(p) / (ln2 * ln2)
	return uint64(math.Ceil(bits))
}

// SuggestNumHashes calculates the optimal number of hash functions from target FPP.
//
// Formula: k = ceil(-ln(p) / ln(2))
// where p = target false positive probability
func SuggestNumHashes(targetFpp float64) uint16 {
	k := -math.Log(targetFpp) / math.Ln2
	return uint16(math.Ceil(k))
}

// SuggestNumHashesFromSize calculates optimal number of hash functions from filter size.
//
// Formula: k = ceil((m/n) * ln(2))
// where m = number of bits, n = number of items
func SuggestNumHashesFromSize(maxDistinctItems, numFilterBits uint64) uint16 {
	if maxDistinctItems == 0 {
		return 1
	}
	ratio := float64(numFilterBits) / float64(maxDistinctItems)
	k := ratio * math.Ln2
	result := uint16(math.Ceil(k))
	if result == 0 {
		return 1
	}
	return result
}

// GenerateRandomSeed generates a cryptographically random seed value.
func GenerateRandomSeed() (uint64, error) {
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	if err != nil {
		return 0, fmt.Errorf("failed to generate random seed: %w", err)
	}
	return binary.LittleEndian.Uint64(buf), nil
}

// NewBloomFilterFromSlice deserializes a Bloom filter from a byte slice.
//
// The byte slice must contain a valid serialized Bloom filter in the
// DataSketches binary format. This format is compatible with C++ and Java
// implementations.
//
// Returns an error if the data is invalid or corrupted.
func NewBloomFilterFromSlice(bytes []byte) (BloomFilter, error) {
	// Validate minimum size
	if len(bytes) < preambleEmptyBytes {
		return nil, fmt.Errorf("insufficient data: need at least %d bytes, got %d", preambleEmptyBytes, len(bytes))
	}

	// Extract and validate preamble fields
	pLongs := extractPreambleLongs(bytes)
	sVer := extractSerVer(bytes)
	fID := extractFamilyID(bytes)
	flags := extractFlags(bytes)
	numHashes := extractNumHashes(bytes)
	seed := extractSeed(bytes)
	bitArrayLength := extractBitArrayLength(bytes)

	// Validate serialization version
	if sVer != serVer {
		return nil, fmt.Errorf("unsupported serialization version: %d (expected %d)", sVer, serVer)
	}

	// Validate family ID
	if fID != familyID {
		return nil, fmt.Errorf("invalid family ID: %d (expected %d)", fID, familyID)
	}

	// Validate preamble longs
	isEmpty := isEmptyFlag(flags)
	expectedPreambleLongs := uint8(preambleLongsEmpty)
	if !isEmpty {
		expectedPreambleLongs = uint8(preambleLongsStandard)
	}
	if pLongs != expectedPreambleLongs {
		return nil, fmt.Errorf("invalid preamble longs: %d (expected %d for empty=%v)", pLongs, expectedPreambleLongs, isEmpty)
	}

	// Validate numHashes
	if numHashes == 0 {
		return nil, fmt.Errorf("numHashes must be positive")
	}

	// Calculate capacity
	capacityBits := uint64(bitArrayLength) * 64

	// Create filter structure
	bf := &bloomFilterImpl{
		seed:         seed,
		numHashes:    numHashes,
		isDirty:      false,
		capacityBits: capacityBits,
		numBitsSet:   0,
		bitArray:     make([]uint64, bitArrayLength),
	}

	// Handle empty filter
	if isEmpty {
		if len(bytes) != preambleEmptyBytes {
			return nil, fmt.Errorf("empty filter size mismatch: got %d bytes, expected %d", len(bytes), preambleEmptyBytes)
		}
		return bf, nil
	}

	// Handle non-empty filter
	expectedSize := preambleBytes + int(bitArrayLength)*8
	if len(bytes) != expectedSize {
		return nil, fmt.Errorf("non-empty filter size mismatch: got %d bytes, expected %d", len(bytes), expectedSize)
	}

	// Extract num bits set
	numBitsSet := extractNumBitsSet(bytes)
	if numBitsSet == dirtyBitsValue {
		// Need to recount
		bf.isDirty = true
	} else {
		bf.numBitsSet = numBitsSet
	}

	// Read bit array
	for i := 0; i < int(bitArrayLength); i++ {
		offset := bitArrayOffset + i*8
		bf.bitArray[i] = binary.LittleEndian.Uint64(bytes[offset:])
	}

	// Recount if dirty
	if bf.isDirty {
		bf.numBitsSet = countBitsSet(bf.bitArray)
		bf.isDirty = false
	}

	return bf, nil
}

// roundCapacity rounds the number of bits up to the nearest multiple of 64.
func roundCapacity(numBits uint64) uint64 {
	return (numBits + 63) & ^uint64(63)
}
