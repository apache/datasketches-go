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

package sampling

import (
	"encoding/binary"
	"errors"
	"math/rand"
)

// ResizeFactor controls how the internal array grows.
// Note: Go's slice append has automatic resizing, so this is kept for
// API compatibility with the Java version. Can be removed if not needed.
type ResizeFactor int

const (
	ResizeX1 ResizeFactor = 1
	ResizeX2 ResizeFactor = 2
	ResizeX4 ResizeFactor = 4
	ResizeX8 ResizeFactor = 8

	defaultResizeFactor = ResizeX8
	minK                = 1
)

// ReservoirItemsSketch provides a uniform random sample of items
// from a stream of unknown size using the reservoir sampling algorithm.
//
// The algorithm works in two phases:
//   - Initial phase (n < k): all items are stored
//   - Steady state (n >= k): each new item replaces a random item with probability k/n
//
// This ensures each item has equal probability k/n of being in the final sample.
type ReservoirItemsSketch[T any] struct {
	k    int   // maximum reservoir size
	n    int64 // total items seen
	data []T   // reservoir storage
}

// NewReservoirItemsSketch creates a new reservoir sketch with the given capacity k.
func NewReservoirItemsSketch[T any](k int) (*ReservoirItemsSketch[T], error) {
	if k < minK {
		return nil, errors.New("k must be at least 1")
	}

	return &ReservoirItemsSketch[T]{
		k:    k,
		n:    0,
		data: make([]T, 0, min(k, int(defaultResizeFactor))),
	}, nil
}

// Update adds an item to the sketch using reservoir sampling algorithm.
func (s *ReservoirItemsSketch[T]) Update(item T) {
	if s.n < int64(s.k) {
		// Initial phase: store all items until reservoir is full
		s.data = append(s.data, item)
	} else {
		// Steady state: replace with probability k/n
		j := rand.Int63n(s.n + 1)
		if j < int64(s.k) {
			s.data[j] = item
		}
	}
	s.n++
}

// K returns the maximum reservoir capacity.
func (s *ReservoirItemsSketch[T]) K() int {
	return s.k
}

// N returns the total number of items seen by the sketch.
func (s *ReservoirItemsSketch[T]) N() int64 {
	return s.n
}

// NumSamples returns the number of items currently in the reservoir.
func (s *ReservoirItemsSketch[T]) NumSamples() int {
	return len(s.data)
}

// Samples returns a copy of the items in the reservoir.
func (s *ReservoirItemsSketch[T]) Samples() []T {
	result := make([]T, len(s.data))
	copy(result, s.data)
	return result
}

// IsEmpty returns true if no items have been seen.
func (s *ReservoirItemsSketch[T]) IsEmpty() bool {
	return s.n == 0
}

// Reset clears the sketch while preserving capacity k.
func (s *ReservoirItemsSketch[T]) Reset() {
	s.n = 0
	s.data = s.data[:0]
}

// Preamble constants for serialization
const (
	preambleIntsShort = 1  // empty sketch
	preambleIntsLong  = 3  // non-empty sketch
	serVer            = 2  // serialization version
	familyID          = 13 // reservoir items family
)

// ToByteArray serializes the sketch to a byte array using the provided SerDe.
func (s *ReservoirItemsSketch[T]) ToByteArray(serde ItemsSerDe[T]) ([]byte, error) {
	if s.IsEmpty() {
		// Empty sketch: minimal preamble
		buf := make([]byte, 8)
		buf[0] = preambleIntsShort
		buf[1] = serVer
		buf[2] = familyID
		// bytes 4-7: k as int32
		binary.LittleEndian.PutUint32(buf[4:], uint32(s.k))
		return buf, nil
	}

	// Serialize items
	itemsBytes, err := serde.SerializeToBytes(s.data)
	if err != nil {
		return nil, err
	}

	// Non-empty sketch
	preambleBytes := preambleIntsLong * 8
	buf := make([]byte, preambleBytes+len(itemsBytes))

	// Preamble
	buf[0] = preambleIntsLong
	buf[1] = serVer
	buf[2] = familyID
	// byte 3: reserved
	binary.LittleEndian.PutUint32(buf[4:], uint32(s.k))
	binary.LittleEndian.PutUint64(buf[8:], uint64(s.n))
	binary.LittleEndian.PutUint32(buf[16:], uint32(len(s.data)))
	// bytes 20-23 reserved

	// Data
	copy(buf[preambleBytes:], itemsBytes)

	return buf, nil
}

// NewReservoirItemsSketchFromSlice deserializes a sketch from a byte array using the provided SerDe.
func NewReservoirItemsSketchFromSlice[T any](data []byte, serde ItemsSerDe[T]) (*ReservoirItemsSketch[T], error) {
	if len(data) < 8 {
		return nil, errors.New("data too short")
	}

	preambleInts := int(data[0])
	ver := data[1]
	family := data[2]
	k := int(binary.LittleEndian.Uint32(data[4:]))

	if ver != serVer {
		return nil, errors.New("unsupported serialization version")
	}
	if family != familyID {
		return nil, errors.New("wrong sketch family")
	}

	if preambleInts == preambleIntsShort {
		// Empty sketch
		return NewReservoirItemsSketch[T](k)
	}

	if len(data) < preambleIntsLong*8 {
		return nil, errors.New("data too short for non-empty sketch")
	}

	n := int64(binary.LittleEndian.Uint64(data[8:]))
	numSamples := int(binary.LittleEndian.Uint32(data[16:]))

	preambleBytes := preambleIntsLong * 8
	itemsData := data[preambleBytes:]

	items, err := serde.DeserializeFromBytes(itemsData, numSamples)
	if err != nil {
		return nil, err
	}

	return &ReservoirItemsSketch[T]{
		k:    k,
		n:    n,
		data: items,
	}, nil
}
