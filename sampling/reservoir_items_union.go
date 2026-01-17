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
//
// The union maintains statistical correctness by:
// - Dynamically choosing merge direction (lighter sketch merges into heavier)
// - Using weighted sampling with correct probability formula
// - Preserving the smaller K when merging sketches in sampling mode
type ReservoirItemsUnion[T any] struct {
	maxK   int                      // maximum k for the union
	gadget *ReservoirItemsSketch[T] // internal sketch (may be nil initially)
}

// NewReservoirItemsUnion creates a new union with the specified maximum k.
func NewReservoirItemsUnion[T any](maxK int) (*ReservoirItemsUnion[T], error) {
	if maxK < minK {
		return nil, errors.New("maxK must be at least 1")
	}

	return &ReservoirItemsUnion[T]{
		maxK:   maxK,
		gadget: nil, // Start with nil gadget, will be initialized on first update
	}, nil
}

// Update adds a single item to the union.
func (u *ReservoirItemsUnion[T]) Update(item T) {
	if u.gadget == nil {
		u.gadget, _ = NewReservoirItemsSketch[T](u.maxK)
	}
	u.gadget.Update(item)
}

// UpdateSketch merges another sketch into the union.
// This implements Java's update(ReservoirItemsSketch) with twoWayMergeInternal logic.
func (u *ReservoirItemsUnion[T]) UpdateSketch(sketch *ReservoirItemsSketch[T]) {
	if sketch == nil || sketch.IsEmpty() {
		return
	}

	// Downsample if input K > maxK
	ris := sketch
	if sketch.K() > u.maxK {
		ris = sketch.DownsampledCopy(u.maxK)
	}

	// Initialize gadget if empty
	if u.gadget == nil || u.gadget.IsEmpty() {
		u.createNewGadget(ris)
		return
	}

	u.twoWayMergeInternal(ris)
}

// createNewGadget initializes the gadget based on the source sketch.
// Matches Java's createNewGadget logic:
// - If source K < maxK AND in exact mode (N <= K): create maxK gadget and merge items
// - Otherwise: use source directly (preserving its K)
func (u *ReservoirItemsUnion[T]) createNewGadget(source *ReservoirItemsSketch[T]) {
	if source.K() < u.maxK && source.N() <= int64(source.K()) {
		// Exact mode with K < maxK: upgrade to maxK and merge all items
		u.gadget, _ = NewReservoirItemsSketch[T](u.maxK)
		for i := 0; i < source.NumSamples(); i++ {
			u.gadget.Update(source.getValueAtPosition(i))
		}
	} else {
		// K >= maxK OR sampling mode: preserve source's K
		u.gadget = source.Copy()
	}
}

// twoWayMergeInternal performs the merge based on the state of both sketches.
// This implements Java's twoWayMergeInternal logic.
func (u *ReservoirItemsUnion[T]) twoWayMergeInternal(source *ReservoirItemsSketch[T]) {
	if source.N() <= int64(source.K()) {
		// Case 1: source is in exact mode - use standard merge
		u.twoWayMergeInternalStandard(source)
	} else if u.gadget.N() < int64(u.gadget.K()) {
		// Case 2: gadget is in exact mode, source is in sampling mode
		// Swap: merge gadget into source (source becomes new gadget)
		tmp := u.gadget
		u.gadget = source.Copy()
		u.twoWayMergeInternalStandard(tmp)
	} else if source.GetImplicitSampleWeight() < float64(u.gadget.N())/float64(u.gadget.K()-1) {
		// Case 3: both in sampling mode, source is "lighter"
		// Merge source into gadget
		u.twoWayMergeInternalWeighted(source)
	} else {
		// Case 4: both in sampling mode, gadget is "lighter"
		// Swap: merge gadget into source
		tmp := u.gadget
		u.gadget = source.Copy()
		u.twoWayMergeInternalWeighted(tmp)
	}
}

// twoWayMergeInternalStandard merges a sketch in exact mode (N <= K) into gadget.
// Simply updates gadget with each item from source.
func (u *ReservoirItemsUnion[T]) twoWayMergeInternalStandard(source *ReservoirItemsSketch[T]) {
	for i := 0; i < source.NumSamples(); i++ {
		u.gadget.Update(source.getValueAtPosition(i))
	}
}

// twoWayMergeInternalWeighted merges a "lighter" sketch into gadget using weighted sampling.
// Uses the correct probability formula: P = (K * w) / targetTotal
func (u *ReservoirItemsUnion[T]) twoWayMergeInternalWeighted(source *ReservoirItemsSketch[T]) {
	numSourceSamples := source.K()
	sourceItemWeight := float64(source.N()) / float64(numSourceSamples)
	rescaledProb := float64(u.gadget.K()) * sourceItemWeight
	targetTotal := float64(u.gadget.N())
	tgtK := u.gadget.K()

	for i := 0; i < numSourceSamples; i++ {
		targetTotal += sourceItemWeight

		// p(keep_new_item) = (k * w) / newTotal
		rescaledFlip := targetTotal * rand.Float64()
		if rescaledFlip < rescaledProb {
			slotNo := rand.Intn(tgtK)
			u.gadget.insertValueAtPosition(source.getValueAtPosition(i), slotNo)
		}
	}

	u.gadget.forceIncrementItemsSeen(source.N())
}

// Result returns a copy of the internal sketch.
func (u *ReservoirItemsUnion[T]) Result() (*ReservoirItemsSketch[T], error) {
	if u.gadget == nil {
		// Return empty sketch with maxK
		return NewReservoirItemsSketch[T](u.maxK)
	}
	return u.gadget.Copy(), nil
}

// MaxK returns the maximum k for this union.
func (u *ReservoirItemsUnion[T]) MaxK() int {
	return u.maxK
}

// Reset clears the union.
func (u *ReservoirItemsUnion[T]) Reset() {
	u.gadget = nil
}
