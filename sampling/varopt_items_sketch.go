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
	"fmt"
	"iter"
	"math"
	"math/rand"
	"slices"
	"strings"

	"github.com/apache/datasketches-go/internal"
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
	data    []T       // stored items
	weights []float64 // corresponding weights for each item (-1.0 indicates R region)

	// The following array is absent in a varopt sketch, and notionally present in a gadget
	// (although it really belongs in the unioning object). If the array were to be made explicit,
	// some additional coding would need to be done to ensure that all of the necessary data motion
	// occurs and is properly tracked.
	marks []bool

	k            int     // maximum sample size (user-configured)
	n            int64   // total number of items processed
	h            int     // number of items in H (heavy/heap) region
	m            int     // number of items in middle region (during candidate set operations)
	r            int     // number of items in R (reservoir) region
	totalWeightR float64 // total weight of items in R region

	// resize factor for array growth
	rf ResizeFactor

	// Following int is:
	//  1. Zero (for a varopt sketch)
	//  2. Count of marked items in H region, if part of a unioning algo's gadget
	numMarksInH uint32
}

const (
	// VarOpt specific constants
	varOptDefaultResizeFactor = ResizeX8
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

func NewVarOptItemsSketch[T any](k uint, opts ...VarOptOption) (*VarOptItemsSketch[T], error) {
	if k < 1 || k > varOptMaxK {
		return nil, errors.New("k must be at least 1 and less than 2^31 - 1")
	}

	cfg := &varOptConfig{
		resizeFactor: varOptDefaultResizeFactor,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	ceilingLgK := math.Log2(float64(internal.CeilPowerOf2(int(k))))
	initialLgSize := startingSubMultiple(int(ceilingLgK), int(cfg.resizeFactor), minLgArrItems)
	currItemsAlloc := adjustedSamplingAllocationSize(int(k), 1<<initialLgSize)
	if uint(currItemsAlloc) == k {
		currItemsAlloc++
	}

	return &VarOptItemsSketch[T]{
		k:            int(k),
		n:            0,
		h:            0,
		m:            0,
		r:            0,
		totalWeightR: 0.0,
		data:         make([]T, 0, currItemsAlloc),
		weights:      make([]float64, 0, currItemsAlloc),
		rf:           cfg.resizeFactor,
		numMarksInH:  0,
	}, nil
}

// K returns the configured maximum sample size.
func (s *VarOptItemsSketch[T]) K() int { return s.k }

// N returns the total number of items processed by the sketch.
func (s *VarOptItemsSketch[T]) N() int64 { return s.n }

// NumSamples returns the number of items currently retained in the sketch.
func (s *VarOptItemsSketch[T]) NumSamples() int {
	return min(s.k, s.h+s.r)
}

// IsEmpty returns true if the sketch has not processed any items.
func (s *VarOptItemsSketch[T]) IsEmpty() bool { return s.n == 0 }

// Reset clears the sketch to its initial empty state while preserving k.
func (s *VarOptItemsSketch[T]) Reset() {
	ceilingLgK := math.Log2(float64(internal.CeilPowerOf2(s.k)))
	initialLgSize := startingSubMultiple(int(ceilingLgK), int(s.rf), minLgArrItems)
	currItemsAlloc := adjustedSamplingAllocationSize(s.k, 1<<initialLgSize)
	if currItemsAlloc == s.k {
		currItemsAlloc++
	}

	s.n = 0
	s.h = 0
	s.m = 0
	s.r = 0
	s.totalWeightR = 0.0
	s.numMarksInH = 0

	s.data = make([]T, 0, currItemsAlloc)
	s.weights = make([]float64, 0, currItemsAlloc)
	if s.marks != nil {
		s.marks = make([]bool, 0, currItemsAlloc)
	}
}

// H returns the number of items in the H (heavy) region.
func (s *VarOptItemsSketch[T]) H() int { return s.h }

// R returns the number of items in the R (reservoir) region.
func (s *VarOptItemsSketch[T]) R() int { return s.r }

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
			tau := s.tau()
			for i := s.h + 1; i <= s.k; i++ {
				if !yield(Sample[T]{Item: s.data[i], Weight: tau}) {
					return
				}
			}
		}
	}
}

// peekMin returns the minimum weight in the H region (heap root).
func (s *VarOptItemsSketch[T]) peekMin() (float64, error) {
	if s.h == 0 {
		return 0, errors.New("h = 0 when checking min in H region")
	}
	return s.weights[0], nil
}

// Update adds an item with the given weight to the sketch.
// Weight must be positive and finite.
func (s *VarOptItemsSketch[T]) Update(item T, weight float64) error {
	return s.update(item, weight, false)
}

func (s *VarOptItemsSketch[T]) update(item T, weight float64, mark bool) error {
	if weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		return errors.New("weight must be strictly positive and finite")
	}

	s.n++

	if s.r == 0 {
		// exact mode (warmup)
		return s.updateWarmupPhase(item, weight, mark)
	}

	var minWeight float64
	if s.h != 0 {
		var err error
		minWeight, err = s.peekMin()
		if err != nil {
			return err
		}

		if minWeight < s.tau() {
			return errors.New("sketch not in valid estimation mode")
		}
	}

	// what tau would be if deletion candidates turn out to be R plus the new item
	// NOTE: (r_ + 1) - 1 is intentional
	hypotheticalTau := (weight + s.totalWeightR) / (float64(s.r+1) - 1)

	// is new item's turn to be considered for reservoir?
	condition1 := s.h == 0 || weight <= minWeight

	// is new item light enough for reservoir?
	condition2 := weight < hypotheticalTau

	if condition1 && condition2 {
		return s.updateLight(item, weight, mark)
	} else if s.r == 1 {
		return s.updateHeavyREqualsTo1(item, weight, mark)
	}
	return s.updateHeavyGeneral(item, weight, mark)
}

// updateWarmupPhase handles the warmup phase when r=0.
func (s *VarOptItemsSketch[T]) updateWarmupPhase(item T, weight float64, mark bool) error {
	if s.h >= cap(s.data) {
		s.growArrays()
	}

	// store items until full
	s.data = append(s.data, item)
	s.weights = append(s.weights, weight)
	if s.marks != nil {
		s.marks = append(s.marks, mark)
	}
	s.h++

	if mark {
		s.numMarksInH++
	}

	// check if need to transition to estimation mode
	if s.h > s.k {
		return s.transitionFromWarmup()
	}
	return nil
}

// transitionFromWarmup converts from warmup (exact) mode to estimation mode.
func (s *VarOptItemsSketch[T]) transitionFromWarmup() error {
	// Move the 2 lightest items from H to M
	// But the lighter really belongs in R, so update counts to reflect that
	if err := s.heapify(); err != nil {
		return err
	}
	if err := s.popMinToMRegion(); err != nil {
		return err
	}
	if err := s.popMinToMRegion(); err != nil {
		return err
	}

	// The lighter of the two really belongs in R
	s.m--
	s.r++

	if s.h != (s.k-1) || s.m != 1 || s.r != 1 {
		return errors.New("invalid state for transitioning from warmup")
	}

	// Update total weight in R and then, having grabbed the value, overwrite
	// in weight_ array to help make bugs more obvious
	s.totalWeightR = s.weights[s.k]
	s.weights[s.k] = -1.0 // mark as R region item

	// The two lightest items are a valid initial candidate set
	// weights[k-1] is in M, weights[k] (now -1) represents R
	return s.growCandidateSet(s.weights[s.k-1]+s.totalWeightR, 2)
}

// NOTE: In the "light" case the new item has weight <= old_tau, so
// would appear to the right of the R items in a hypothetical reverse-sorted
// list. It is easy to prove that it is light enough to be part of this
// round's downsampling
func (s *VarOptItemsSketch[T]) updateLight(item T, weight float64, mark bool) error {
	if s.r == 0 || (s.r+s.h) != s.k {
		return errors.New("invalid sketch state during light warmup")
	}

	// The M slot is at index h (the gap)
	mSlot := s.h
	s.data[mSlot] = item
	s.weights[mSlot] = weight
	if s.marks != nil {
		s.marks[mSlot] = mark
	}
	s.m++

	return s.growCandidateSet(s.totalWeightR+weight, s.r+1)
}

// NOTE: In the "heavy" case the new item has weight > old_tau, so would
// appear to the left of items in R in a hypothetical reverse-sorted list and
// might or might not be light enough be part of this round's downsampling.
// [After first splitting off the R=1 case] we greatly simplify the code by
// putting the new item into the H heap whether it needs to be there or not.
// In other words, it might go into the heap and then come right back out,
// but that should be okay because pseudo_heavy items cannot predominate
// in long streams unless (max wt) / (min wt) > o(exp(N))
func (s *VarOptItemsSketch[T]) updateHeavyGeneral(item T, weight float64, mark bool) error {
	if s.r < 2 || s.m != 0 || (s.r+s.h) != s.k {
		return errors.New("invalid sketch state during heavy general update")
	}

	// Put into H (may come back out momentarily)
	s.push(item, weight, mark)

	return s.growCandidateSet(s.totalWeightR, s.r)
}

// NOTE: The analysis of this case is similar to that of the general heavy case.
// The one small technical difference is that since R < 2, we must grab an M item
// to have a valid starting point for growCandidateSet
func (s *VarOptItemsSketch[T]) updateHeavyREqualsTo1(item T, weight float64, mark bool) error {
	if s.r != 1 || s.m != 0 || (s.r+s.h) != s.k {
		return errors.New("invalid sketch state during heavy r=1 update")
	}

	s.push(item, weight, mark)                  // new item into H
	if err := s.popMinToMRegion(); err != nil { // pop lightest back into M
		return err
	}

	// Any set of two items is downsample-able to one item,
	// so the two lightest items are a valid starting point for the following
	mSlot := s.k - 1 // The M slot is at k-1 (array is k+1, 1 in R)
	return s.growCandidateSet(s.weights[mSlot]+s.totalWeightR, 2)
}

func (s *VarOptItemsSketch[T]) push(item T, weight float64, mark bool) {
	s.data[s.h] = item
	s.weights[s.h] = weight
	if s.marks != nil {
		s.marks[s.h] = mark
		if mark {
			s.numMarksInH++
		}
	}
	s.h++

	s.restoreTowardsRoot(s.h - 1)
}

// popMinToMRegion moves the minimum item from H to M region.
func (s *VarOptItemsSketch[T]) popMinToMRegion() error {
	if s.h == 0 || (s.h+s.m+s.r) != (s.k+1) {
		return errors.New("invalid heap state popping min to M region")
	}

	if s.h == 1 { // just update bookkeeping
		s.m++
		s.h--
	} else {
		tgt := s.h - 1
		s.swap(0, tgt)
		s.m++
		s.h--

		if err := s.restoreTowardsLeaves(0); err != nil {
			return err
		}
	}

	if s.isMarked() {
		s.numMarksInH--
	}
	return nil
}

func (s *VarOptItemsSketch[T]) isMarked() bool {
	return s.marks != nil && s.marks[s.h]
}

// NOTE: When entering here we should be in a well-characterized state where the
// new item has been placed in either h or m and we have a valid but not necessarily
// maximal sampling plan figured out. The array is completely full at this point.
// Everyone in h and m has an explicit weight. The candidates are right-justified
// and are either just the r set or the r set + exactly one m item. The number
// of cands is at least 2. We will now grow the candidate set as much as possible
// by pulling sufficiently light items from h to m.
func (s *VarOptItemsSketch[T]) growCandidateSet(wtCands float64, numCands int) error {
	if (s.h+s.m+s.r != s.k+1) || numCands < 1 || numCands != (s.m+s.r) || s.m >= 2 {
		return errors.New("invariant violated when growing candidate set")
	}

	for s.h > 0 {
		nextWt, err := s.peekMin()
		if err != nil {
			return err
		}

		nextTotWt := wtCands + nextWt

		// test for strict lightness: nextWt * numCands < nextTotWt
		if nextWt*float64(numCands) < nextTotWt {
			wtCands = nextTotWt
			numCands++
			if err := s.popMinToMRegion(); err != nil {
				return err
			}
		} else {
			break
		}
	}

	return s.downsampleCandidateSet(wtCands, numCands)
}

// downsampleCandidateSet downsamples the candidate set to produce final R.
func (s *VarOptItemsSketch[T]) downsampleCandidateSet(wtCands float64, numCands int) error {
	if numCands < 2 || s.h+numCands != s.k+1 {
		return errors.New("invalid numCands when downsampling")
	}

	// Choose which slot to delete
	deleteSlot, err := s.chooseDeleteSlot(wtCands, numCands)
	if err != nil {
		return err
	}

	leftmostCandSlot := s.h
	if deleteSlot < leftmostCandSlot || deleteSlot > s.k {
		return errors.New("invalid delete slot index when downsampling")
	}

	// Overwrite weights for items from M moving into R,
	// to make bugs more obvious. Also needed so anyone reading the
	// weight knows if it's invalid without checking h and m
	stopIdx := leftmostCandSlot + s.m
	for j := leftmostCandSlot; j < stopIdx; j++ {
		s.weights[j] = -1.0
	}

	// The next line works even when delete_slot == leftmost_cand_slot
	s.data[deleteSlot] = s.data[leftmostCandSlot]

	s.m = 0
	s.r = numCands - 1
	s.totalWeightR = wtCands
	return nil
}

// chooseDeleteSlot randomly selects which item to delete from candidates.
func (s *VarOptItemsSketch[T]) chooseDeleteSlot(wtCands float64, numCands int) (int, error) {
	if s.r == 0 {
		return 0, errors.New("choosing delete slot while in exact mode")
	}

	switch s.m {
	case 0:
		// this happens if we insert a really heavy item
		return s.pickRandomSlotInR()
	case 1:
		// Check if we keep the item in M or pick one from R
		// p(keep) = (numCands - 1) * wtM / wtCands
		wtMCand := s.weights[s.h] // slot of item in M is h
		if wtCands*s.randFloat64NonZero() < float64(numCands-1)*wtMCand {
			return s.pickRandomSlotInR() // keep item in M
		}
		return s.h, nil // delete item in M
	default:
		// General case with multiple M items
		deleteSlot := s.chooseWeightedDeleteSlot(wtCands, numCands)
		firstRSlot := s.h + s.m
		if deleteSlot == firstRSlot {
			return s.pickRandomSlotInR()
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

// pickRandomSlotInR returns a random index from the R region.
func (s *VarOptItemsSketch[T]) pickRandomSlotInR() (int, error) {
	if s.r == 0 {
		return 0, errors.New("r == 0 when picking slot in R region")
	}

	offset := s.h + s.m
	if s.r == 1 {
		return offset, nil
	}
	return offset + rand.Intn(s.r), nil
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

// heapify converts data and weights to heap.
func (s *VarOptItemsSketch[T]) heapify() error {
	if s.h < 2 {
		return nil
	}

	lastSlot := s.h - 1
	lastNonLeaf := ((lastSlot + 1) / 2) - 1

	for j := lastNonLeaf; j >= 0; j-- {
		if err := s.restoreTowardsLeaves(j); err != nil {
			return err
		}
	}
	return nil
}

// siftDown restores heap property by moving element down.
func (s *VarOptItemsSketch[T]) restoreTowardsLeaves(slotIn int) error {
	lastSlot := s.h - 1
	if s.h == 0 || slotIn > lastSlot {
		return errors.New("invalid heap state")
	}

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

	return nil
}

func (s *VarOptItemsSketch[T]) tau() float64 {
	if s.r == 0 {
		return math.NaN()
	}
	return s.totalWeightR / float64(s.r)
}

func (s *VarOptItemsSketch[T]) restoreTowardsRoot(slotIn int) {
	slot := slotIn
	p := ((slot + 1) / 2) - 1 // parent

	for slot > 0 && s.weights[slot] < s.weights[p] {
		s.swap(slot, p)
		slot = p
		p = ((slot + 1) / 2) - 1
	}
}

func (s *VarOptItemsSketch[T]) swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
	s.weights[i], s.weights[j] = s.weights[j], s.weights[i]

	if s.marks != nil {
		s.marks[i], s.marks[j] = s.marks[j], s.marks[i]
	}
}

func (s *VarOptItemsSketch[T]) growArrays() {
	prevSize := cap(s.data)
	newSize := s.adjustedSize(s.k, prevSize<<int(s.rf))
	if newSize == s.k {
		newSize++ // need space for the gap
	}

	if prevSize < newSize {
		s.data = slices.Grow(s.data, newSize)
		s.weights = slices.Grow(s.weights, newSize)

		if s.marks != nil {
			s.marks = slices.Grow(s.marks, newSize)
		}
	}
}

// adjustedSize returns the appropriate array size.
func (s *VarOptItemsSketch[T]) adjustedSize(maxSize, resizeTarget int) int {
	if maxSize < (resizeTarget << 1) {
		return maxSize
	}
	return resizeTarget
}

// String returns a human-readable summary of this sketch.
func (s *VarOptItemsSketch[T]) String() string {
	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString("### VarOptItemsSketch SUMMARY: \n")
	sb.WriteString(fmt.Sprintf("   k            : %d\n", s.k))
	sb.WriteString(fmt.Sprintf("   h            : %d\n", s.h))
	sb.WriteString(fmt.Sprintf("   r            : %d\n", s.r))
	sb.WriteString(fmt.Sprintf("   weight_r     : %g\n", s.totalWeightR))
	sb.WriteString(fmt.Sprintf("   Current size : %d\n", cap(s.data)))
	sb.WriteString(fmt.Sprintf("   Resize factor: %v\n", s.rf))
	sb.WriteString("### END SKETCH SUMMARY\n")
	return sb.String()
}

// EstimateSubsetSum computes an estimated subset sum from the entire stream for objects matching a given
// predicate. Provides a lower bound, estimate, and upper bound using a target of 2 standard deviations.
//
// NOTE: This is technically a heuristic method, and tries to err on the conservative side.
//
// predicate: A predicate to use when identifying items.
// Returns a summary object containing the estimate, upper and lower bounds, and the total sketch weight.
func (s *VarOptItemsSketch[T]) EstimateSubsetSum(predicate func(T) bool) (SampleSubsetSummary, error) {
	if s.n == 0 {
		return SampleSubsetSummary{}, nil
	}

	var (
		weightSumInH  = 0.0
		trueWeightInH = 0.0
		idx           = 0
	)
	for idx < s.h {
		weight := s.weights[idx]

		weightSumInH += weight

		if predicate(s.data[idx]) {
			trueWeightInH += weight
		}

		idx++
	}

	// if only heavy items, we have an exact answer
	if s.r == 0 {
		return SampleSubsetSummary{
			LowerBound:        trueWeightInH,
			Estimate:          trueWeightInH,
			UpperBound:        trueWeightInH,
			TotalSketchWeight: trueWeightInH,
		}, nil
	}

	numSampled := s.n - int64(s.h)
	effectiveSamplingRate := float64(s.r) / float64(numSampled)
	if effectiveSamplingRate < 0 || effectiveSamplingRate > 1.0 {
		return SampleSubsetSummary{}, errors.New("invalid sampling rate outside [0.0, 1.0]")
	}

	trueRCount := 0
	idx++ // skip the gap
	for idx < (s.k + 1) {
		if predicate(s.data[idx]) {
			trueRCount++
		}

		idx++
	}

	lowerBoundTrueFraction, err := pseudoHypergeometricLowerBoundOnP(uint64(s.r), uint64(trueRCount), effectiveSamplingRate)
	if err != nil {
		return SampleSubsetSummary{}, err
	}
	upperTrueFraction, err := pseudoHypergeometricUpperBoundOnP(uint64(s.r), uint64(trueRCount), effectiveSamplingRate)
	if err != nil {
		return SampleSubsetSummary{}, err
	}
	estimatedTrueFraction := float64(trueRCount) / float64(s.r)
	return SampleSubsetSummary{
		LowerBound:        trueWeightInH + s.totalWeightR*lowerBoundTrueFraction,
		Estimate:          trueWeightInH + s.totalWeightR*estimatedTrueFraction,
		UpperBound:        trueWeightInH + s.totalWeightR*upperTrueFraction,
		TotalSketchWeight: weightSumInH + s.totalWeightR,
	}, nil
}
