//go:build experimental
// +build experimental

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
	"iter"
	"math"
	"math/rand"
)

// VarOptItemsSketch implements variance-optimal weighted sampling.
//
// This sketch samples weighted items from a stream with optimal variance
// for subset sum estimation. The algorithm maintains two regions:
//   - H region (heavy): Items with weight >= tau (stored in a min-heap)
//   - R region (reservoir): Items with weight < tau (sampled proportionally)
//
// The array layout is: [H region: 0..h) [gap/M region: h..h+m) [R region: h+m..h+m+r)
// In steady state, m=0 and h+r=k. The gap slot at position h is used during updates.
//
// When all weights are equal (e.g., 1.0), this reduces to standard reservoir sampling.
//
// Reference: Cohen et al., "Efficient Stream Sampling for Variance-Optimal
// Estimation of Subset Sums", SIAM J. Comput. 40(5): 1402-1431, 2011.
type VarOptItemsSketch[T any] struct {
	k            int       // maximum sample size (user-configured)
	n            int64     // total number of items processed
	h            int       // number of items in H (heavy/heap) region
	m            int       // number of items in middle region (during candidate set operations)
	r            int       // number of items in R (reservoir) region
	totalWeightR float64   // total weight of items in R region
	data         []T       // stored items
	weights      []float64 // corresponding weights for each item (-1.0 indicates R region)

	// resize factor for array growth
	rf ResizeFactor

	// current allocated capacity
	allocatedSize int
}

const (
	// VarOpt specific constants
	varOptDefaultResizeFactor = ResizeX8
	varOptMinLgK              = 3 // minimum log2(k) = 3, so minimum k = 8
	varOptMinK                = 1 << varOptMinLgK
	varOptMaxK                = (1 << 31) - 2 // maximum k value
)

type VarOptOption func(*varOptConfig)

type varOptConfig struct {
	resizeFactor ResizeFactor
}

func WithResizeFactor(rf ResizeFactor) VarOptOption {
	return func(c *varOptConfig) {
		c.resizeFactor = rf
	}
}

func NewVarOptItemsSketch[T any](k int, opts ...VarOptOption) (*VarOptItemsSketch[T], error) {
	if k < varOptMinK {
		return nil, errors.New("k must be at least 8")
	}
	if k > varOptMaxK {
		return nil, errors.New("k is too large")
	}

	cfg := &varOptConfig{
		resizeFactor: varOptDefaultResizeFactor,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	initialSize := int(cfg.resizeFactor)
	if initialSize > k {
		initialSize = k
	}

	return &VarOptItemsSketch[T]{
		k:             k,
		n:             0,
		h:             0,
		m:             0,
		r:             0,
		totalWeightR:  0.0,
		data:          make([]T, 0, initialSize),
		weights:       make([]float64, 0, initialSize),
		rf:            cfg.resizeFactor,
		allocatedSize: initialSize,
	}, nil
}

// K returns the configured maximum sample size.
func (s *VarOptItemsSketch[T]) K() int { return s.k }

// N returns the total number of items processed by the sketch.
func (s *VarOptItemsSketch[T]) N() int64 { return s.n }

// NumSamples returns the number of items currently retained in the sketch.
func (s *VarOptItemsSketch[T]) NumSamples() int { return s.h + s.r }

// IsEmpty returns true if the sketch has not processed any items.
func (s *VarOptItemsSketch[T]) IsEmpty() bool { return s.n == 0 }

// Reset clears the sketch to its initial empty state while preserving k.
func (s *VarOptItemsSketch[T]) Reset() {
	s.n = 0
	s.h = 0
	s.m = 0
	s.r = 0
	s.totalWeightR = 0.0
	s.data = s.data[:0]
	s.weights = s.weights[:0]
}

// H returns the number of items in the H (heavy) region.
func (s *VarOptItemsSketch[T]) H() int { return s.h }

// R returns the number of items in the R (reservoir) region.
func (s *VarOptItemsSketch[T]) R() int { return s.r }

// TotalWeightR returns the total weight of items in the R region.
func (s *VarOptItemsSketch[T]) TotalWeightR() float64 { return s.totalWeightR }

// Sample represents a weighted sample item.
type Sample[T any] struct {
	Item   T
	Weight float64
}

// All returns an iterator over all samples with their adjusted weights.
// For items in H region, the weight is the original weight.
// For items in R region, the weight is tau (totalWeightR / r).
func (s *VarOptItemsSketch[T]) All() iter.Seq[Sample[T]] {
	return func(yield func(Sample[T]) bool) {
		// H region: items with their actual weights
		for i := 0; i < s.h; i++ {
			if !yield(Sample[T]{Item: s.data[i], Weight: s.weights[i]}) {
				return
			}
		}

		// R region: items with weight = tau
		if s.r > 0 {
			tau := s.totalWeightR / float64(s.r)
			rStart := s.h + s.m
			for i := 0; i < s.r; i++ {
				if !yield(Sample[T]{Item: s.data[rStart+i], Weight: tau}) {
					return
				}
			}
		}
	}
}

// inWarmup returns true if the sketch is still in warmup phase (exact mode).
// During warmup, r=0 and we store all items directly in H.
func (s *VarOptItemsSketch[T]) inWarmup() bool {
	return s.r == 0
}

// peekMin returns the minimum weight in the H region (heap root).
func (s *VarOptItemsSketch[T]) peekMin() float64 {
	if s.h == 0 {
		return math.Inf(1)
	}
	return s.weights[0]
}

// Update adds an item with the given weight to the sketch.
// Weight must be positive and finite.
func (s *VarOptItemsSketch[T]) Update(item T, weight float64) error {
	if weight < 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		return errors.New("weight must be nonnegative and finite")
	}
	if weight == 0 {
		return nil // ignore zero weight items
	}

	s.n++

	if s.r == 0 {
		// exact mode (warmup)
		return s.updateWarmupPhase(item, weight)
	}
	// estimation mode
	// what tau would be if deletion candidates = R + new item
	hypotheticalTau := (weight + s.totalWeightR) / float64(s.r) // r+1-1 = r

	// is new item's turn to be considered for reservoir?
	condition1 := s.h == 0 || weight <= s.peekMin()
	// is new item light enough for reservoir?
	condition2 := weight < hypotheticalTau

	if condition1 && condition2 {
		return s.updateLight(item, weight)
	} else if s.r == 1 {
		return s.updateHeavyREq1(item, weight)
	}
	return s.updateHeavyGeneral(item, weight)
}

// updateWarmupPhase handles the warmup phase when r=0.
func (s *VarOptItemsSketch[T]) updateWarmupPhase(item T, weight float64) error {
	if s.h >= cap(s.data) {
		s.growDataArrays()
	}

	// store items as they come in
	if s.h < len(s.data) {
		s.data[s.h] = item
		s.weights[s.h] = weight
	} else {
		s.data = append(s.data, item)
		s.weights = append(s.weights, weight)
	}
	s.h++

	// check if need to transition to estimation mode
	if s.h > s.k {
		return s.transitionFromWarmup()
	}
	return nil
}

// transitionFromWarmup converts from warmup (exact) mode to estimation mode.
func (s *VarOptItemsSketch[T]) transitionFromWarmup() error {
	// Convert to heap and move 2 lightest items to M region
	s.heapify()
	s.popMinToMRegion()
	s.popMinToMRegion()

	// The lighter of the two really belongs in R
	s.m--
	s.r++

	// h should be k-1, m should be 1, r should be 1

	// Update total weight in R (the item at position k)
	s.totalWeightR = s.weights[s.k]
	s.weights[s.k] = -1.0 // mark as R region item

	// The two lightest items are a valid initial candidate set
	// weights[k-1] is in M, weights[k] (now -1) represents R
	return s.growCandidateSet(s.weights[s.k-1]+s.totalWeightR, 2)
}

// updateLight handles a light item (weight <= old_tau) in estimation mode.
func (s *VarOptItemsSketch[T]) updateLight(item T, weight float64) error {
	// The M slot is at index h (the gap)
	mSlot := s.h
	s.data[mSlot] = item
	s.weights[mSlot] = weight
	s.m++

	return s.growCandidateSet(s.totalWeightR+weight, s.r+1)
}

// updateHeavyGeneral handles a heavy item when r >= 2.
func (s *VarOptItemsSketch[T]) updateHeavyGeneral(item T, weight float64) error {
	// Put into H (may come back out momentarily)
	s.push(item, weight)

	return s.growCandidateSet(s.totalWeightR, s.r)
}

// updateHeavyREq1 handles a heavy item when r == 1.
func (s *VarOptItemsSketch[T]) updateHeavyREq1(item T, weight float64) error {
	s.push(item, weight) // new item into H
	s.popMinToMRegion()  // pop lightest back into M

	// The M slot is at k-1 (array is k+1, 1 in R)
	mSlot := s.k - 1
	return s.growCandidateSet(s.weights[mSlot]+s.totalWeightR, 2)
}

// push adds an item to the H region heap.
func (s *VarOptItemsSketch[T]) push(item T, weight float64) {
	// Insert at position h (the gap)
	s.data[s.h] = item
	s.weights[s.h] = weight
	s.h++

	s.siftUp(s.h - 1)
}

// popMinToMRegion moves the minimum item from H to M region.
func (s *VarOptItemsSketch[T]) popMinToMRegion() {
	if s.h == 0 {
		return
	}

	if s.h == 1 {
		s.m++
		s.h--
	} else {
		tgt := s.h - 1
		s.swap(0, tgt)
		s.m++
		s.h--
		s.siftDown(0)
	}
}

// growCandidateSet grows the candidate set by pulling light items from H to M.
func (s *VarOptItemsSketch[T]) growCandidateSet(wtCands float64, numCands int) error {
	for s.h > 0 {
		nextWt := s.peekMin()
		nextTotWt := wtCands + nextWt

		// test for strict lightness: nextWt * numCands < nextTotWt
		if nextWt*float64(numCands) < nextTotWt {
			wtCands = nextTotWt
			numCands++
			s.popMinToMRegion()
		} else {
			break
		}
	}

	return s.downsampleCandidateSet(wtCands, numCands)
}

// downsampleCandidateSet downsamples the candidate set to produce final R.
func (s *VarOptItemsSketch[T]) downsampleCandidateSet(wtCands float64, numCands int) error {
	if numCands < 2 {
		return nil
	}

	// Choose which slot to delete
	deleteSlot, err := s.chooseDeleteSlot(wtCands, numCands)
	if err != nil {
		return err
	}

	leftmostCandSlot := s.h

	// Mark weights for items moving from M to R as -1
	stopIdx := leftmostCandSlot + s.m
	for j := leftmostCandSlot; j < stopIdx; j++ {
		s.weights[j] = -1.0
	}

	// Move the delete slot's content to leftmost candidate position
	// This works even when deleteSlot == leftmostCandSlot
	if deleteSlot != leftmostCandSlot {
		s.data[deleteSlot] = s.data[leftmostCandSlot]
	}

	s.m = 0
	s.r = numCands - 1
	s.totalWeightR = wtCands
	return nil
}

// chooseDeleteSlot randomly selects which item to delete from candidates.
func (s *VarOptItemsSketch[T]) chooseDeleteSlot(wtCands float64, numCands int) (int, error) {
	if s.r == 0 {
		return 0, errors.New("chooseDeleteSlot called while in exact mode (r == 0)")
	}

	if s.m == 0 {
		// All candidates are in R, pick random slot
		return s.randomRIndex(), nil
	} else if s.m == 1 {
		// Check if we keep the item in M or pick one from R
		// p(keep) = (numCands - 1) * wtM / wtCands
		wtMCand := s.weights[s.h] // slot of item in M is h
		if wtCands*s.randFloat64NonZero() < float64(numCands-1)*wtMCand {
			return s.randomRIndex(), nil // keep item in M
		}
		return s.h, nil // delete item in M
	} else {
		// General case with multiple M items
		deleteSlot := s.chooseWeightedDeleteSlot(wtCands, numCands)
		firstRSlot := s.h + s.m
		if deleteSlot == firstRSlot {
			return s.randomRIndex(), nil
		}
		return deleteSlot, nil
	}
}

// chooseWeightedDeleteSlot implements weighted random selection.
func (s *VarOptItemsSketch[T]) chooseWeightedDeleteSlot(wtCands float64, numCands int) int {
	offset := s.h
	finalM := (offset + s.m) - 1
	numToKeep := numCands - 1

	leftSubtotal := 0.0
	rightSubtotal := -wtCands * s.randFloat64NonZero()

	for i := offset; i <= finalM; i++ {
		leftSubtotal += float64(numToKeep) * s.weights[i]
		rightSubtotal += wtCands

		if leftSubtotal < rightSubtotal {
			return i
		}
	}

	// Delete from R
	return finalM + 1
}

// randomRIndex returns a random index from the R region.
func (s *VarOptItemsSketch[T]) randomRIndex() int {
	offset := s.h + s.m
	if s.r == 1 {
		return offset
	}
	return offset + rand.Intn(s.r)
}

// randFloat64NonZero returns a random float64 in (0, 1).
func (s *VarOptItemsSketch[T]) randFloat64NonZero() float64 {
	for {
		r := rand.Float64()
		if r > 0 {
			return r
		}
	}
}

// heapify converts H region to a valid min-heap.
func (s *VarOptItemsSketch[T]) heapify() {
	if s.h < 2 {
		return
	}

	lastSlot := s.h - 1
	lastNonLeaf := ((lastSlot + 1) / 2) - 1

	for j := lastNonLeaf; j >= 0; j-- {
		s.siftDown(j)
	}
}

// siftDown restores heap property by moving element down.
func (s *VarOptItemsSketch[T]) siftDown(slotIn int) {
	lastSlot := s.h - 1
	slot := slotIn
	child := 2*slotIn + 1

	for child <= lastSlot {
		child2 := child + 1
		if child2 <= lastSlot && s.weights[child2] < s.weights[child] {
			child = child2
		}

		if s.weights[slot] <= s.weights[child] {
			break
		}

		s.swap(slot, child)
		slot = child
		child = 2*slot + 1
	}
}

// siftUp restores heap property by moving element up.
func (s *VarOptItemsSketch[T]) siftUp(slotIn int) {
	slot := slotIn
	p := ((slot + 1) / 2) - 1 // parent

	for slot > 0 && s.weights[slot] < s.weights[p] {
		s.swap(slot, p)
		slot = p
		p = ((slot + 1) / 2) - 1
	}
}

// swap exchanges items at two positions.
func (s *VarOptItemsSketch[T]) swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
	s.weights[i], s.weights[j] = s.weights[j], s.weights[i]
}

// growDataArrays increases the capacity of data and weights arrays.
func (s *VarOptItemsSketch[T]) growDataArrays() {
	prevSize := s.allocatedSize
	newSize := s.adjustedSize(s.k, prevSize<<int(s.rf))
	if newSize == s.k {
		newSize++ // need space for the gap
	}

	if newSize > prevSize {
		newData := make([]T, len(s.data), newSize)
		copy(newData, s.data)
		s.data = newData

		newWeights := make([]float64, len(s.weights), newSize)
		copy(newWeights, s.weights)
		s.weights = newWeights

		s.allocatedSize = newSize
	}
}

// adjustedSize returns the appropriate array size.
func (s *VarOptItemsSketch[T]) adjustedSize(maxSize, resizeTarget int) int {
	if resizeTarget <= maxSize {
		return resizeTarget
	}
	return maxSize
}
