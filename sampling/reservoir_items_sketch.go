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
