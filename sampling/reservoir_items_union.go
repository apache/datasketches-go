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

// ReservoirItemsUnion enables merging of multiple ReservoirItemsSketch instances.
// This is useful for distributed sampling where each node maintains a local sketch,
// and the results are merged to get a global sample.
type ReservoirItemsUnion[T any] struct {
	maxK   int                      // maximum k for the union
	gadget *ReservoirItemsSketch[T] // internal sketch
}

// NewReservoirItemsUnion creates a new union with the specified maximum k.
func NewReservoirItemsUnion[T any](maxK int) (*ReservoirItemsUnion[T], error) {
	if maxK < minK {
		return nil, errors.New("maxK must be at least 1")
	}

	gadget, err := NewReservoirItemsSketch[T](maxK)
	if err != nil {
		return nil, err
	}

	return &ReservoirItemsUnion[T]{
		maxK:   maxK,
		gadget: gadget,
	}, nil
}

// Update adds a single item to the union.
func (u *ReservoirItemsUnion[T]) Update(item T) {
	u.gadget.Update(item)
}

// UpdateSketch merges another sketch into the union.
func (u *ReservoirItemsUnion[T]) UpdateSketch(sketch *ReservoirItemsSketch[T]) {
	if sketch == nil || sketch.IsEmpty() {
		return
	}

	samples := sketch.GetSamples()
	srcN := sketch.GetN()

	if u.gadget.IsEmpty() {
		// If gadget is empty, copy the source directly
		for _, v := range samples {
			u.gadget.data = append(u.gadget.data, v)
		}
		u.gadget.n = srcN
		return
	}

	// Merge using weighted sampling
	gadgetN := u.gadget.GetN()
	totalN := gadgetN + srcN
	gadgetK := u.gadget.GetNumSamples()
	targetK := u.maxK

	for _, item := range samples {
		if u.gadget.GetNumSamples() < targetK {
			u.gadget.data = append(u.gadget.data, item)
		} else {
			j := rand.Int63n(totalN)
			if j < int64(targetK) {
				u.gadget.data[j%int64(gadgetK)] = item
			}
		}
	}

	u.gadget.n = totalN
}

// GetResult returns a copy of the internal sketch.
func (u *ReservoirItemsUnion[T]) GetResult() (*ReservoirItemsSketch[T], error) {
	result, err := NewReservoirItemsSketch[T](u.maxK)
	if err != nil {
		return nil, err
	}

	result.k = u.gadget.k
	result.n = u.gadget.n
	result.data = make([]T, len(u.gadget.data))
	copy(result.data, u.gadget.data)

	return result, nil
}

// GetMaxK returns the maximum k for this union.
func (u *ReservoirItemsUnion[T]) GetMaxK() int {
	return u.maxK
}

// Reset clears the union.
func (u *ReservoirItemsUnion[T]) Reset() {
	u.gadget.Reset()
}
