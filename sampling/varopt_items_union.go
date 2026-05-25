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
	"math"
	"strings"
)

// VarOptItemsUnion provides a unioning operation over varopt sketches.
// This union allows the sample size k to float, possibly increasing or
// decreasing as warranted by the available data.
type VarOptItemsUnion[T any] struct {
	gadget        *VarOptItemsSketch[T]
	k             int
	n             int64   // cumulative over all input sketches
	outerTauNumer float64 // outer tau is the largest tau of any input sketch
	outerTauDenom int64   // total cardinality of the same R-zones, or zero if no input sketch was in estimation mode
}

// NewVarOptItemsUnion creates a new union with the specified maximum k.
func NewVarOptItemsUnion[T any](maxK uint) (*VarOptItemsUnion[T], error) {
	gadget, err := newVarOptItemsSketchFromState[T](
		int(maxK), defaultResizeFactor, true,
	)
	if err != nil {
		return nil, err
	}

	return &VarOptItemsUnion[T]{
		k:             int(maxK),
		n:             0,
		outerTauNumer: 0,
		outerTauDenom: 0,
		gadget:        gadget,
	}, nil
}

func (u *VarOptItemsUnion[T]) Update(sketch *VarOptItemsSketch[T]) error {
	if sketch == nil {
		return nil
	}

	n := sketch.N()
	if n == 0 {
		return nil
	}

	u.n += n

	// insert H region items
	for sample := range sketch.hRegionSamplesWithMark() {
		if err := u.gadget.update(sample.Item, sample.Weight, false); err != nil {
			return err
		}
	}

	// insert R region items
	for sample := range sketch.weightCorrRRegionSamples() {
		if err := u.gadget.update(sample.Item, sample.Weight, true); err != nil {
			return err
		}
	}

	if sketch.r > 0 {
		sketchTau := sketch.tau()
		outerTau := u.outerTau()

		switch {
		case u.outerTauDenom == 0:
			// detect first estimation mode sketch and grab its tau
			u.outerTauNumer = sketch.totalWeightR
			u.outerTauDenom = int64(sketch.r)
		case sketchTau > outerTau:
			// switch to a bigger value of outerTau
			u.outerTauNumer = sketch.totalWeightR
			u.outerTauDenom = int64(sketch.r)
		case sketchTau == outerTau:
			u.outerTauNumer += sketch.totalWeightR
			u.outerTauDenom += int64(sketch.r)
		default:
			// do nothing if sketch's tau is smaller than outerTau
		}
	}

	return nil
}

func (u *VarOptItemsUnion[T]) outerTau() float64 {
	if u.outerTauDenom == 0 {
		return 0
	}

	return u.outerTauNumer / float64(u.outerTauDenom)
}

// UpdateReservoirItemsSketch unions a reservoir sketch. The reservoir sample is treated
// as if all items were added with a weight of 1.0.
func (u *VarOptItemsUnion[T]) UpdateReservoirItemsSketch(sketch *ReservoirItemsSketch[T]) error {
	if sketch == nil {
		return nil
	}

	n := sketch.N()
	if n == 0 {
		return nil
	}

	u.n += n

	reservoirK := sketch.K()
	if sketch.N() <= int64(reservoirK) { // exact mode.
		for _, item := range sketch.data {
			if err := u.gadget.update(item, 1.0, false); err != nil {
				return err
			}
		}
		return nil
	}

	// sampling mode.
	reservoirTau := sketch.implicitSampleWeight()

	cumWeight := 0.0
	samples := sketch.data
	for i := 0; i < (reservoirK - 1); i++ {
		if err := u.gadget.update(samples[i], reservoirTau, true); err != nil {
			return err
		}

		cumWeight += reservoirTau
	}

	// correct for any numerical discrepancies with the last item
	if err := u.gadget.update(samples[reservoirK-1], float64(sketch.N())-cumWeight, true); err != nil {
		return err
	}

	outerTau := u.outerTau()
	if u.outerTauDenom == 0 {
		// detect first estimation mode sketch and grab its tau
		u.outerTauNumer = float64(sketch.N())
		u.outerTauDenom = int64(reservoirK)
	} else if reservoirTau > outerTau {
		// switch to a bigger value of outerTau
		u.outerTauNumer = float64(sketch.N())
		u.outerTauDenom = int64(reservoirK)
	} else if reservoirTau == outerTau {
		// Ok if previous equality test isn't quite perfect. Mistakes in either direction should
		// be fairly benign.
		// Without conceptually changing outerTau, update number and denominator. In particular,
		// add the total weight of the incoming reservoir to the running total.
		u.outerTauNumer += float64(sketch.N())
		u.outerTauDenom += int64(reservoirK)
	}

	return nil
}

// Reset resets this sketch to the empty state, but retains the original value of max k.
func (u *VarOptItemsUnion[T]) Reset() {
	u.gadget.Reset()
	u.n = 0
	u.outerTauNumer = 0
	u.outerTauDenom = 0
}

// Strings returns a human-readable summary of the sketch, without items.
func (u *VarOptItemsUnion[T]) String() string {
	if u.gadget == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("### VarOptItemsUnion Summary: ")
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("   Max k: %d", u.k))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("   Gadget summary: %s", u.gadget.String()))
	sb.WriteString("### END UNION SUMMARY")
	sb.WriteString("\n")
	return sb.String()
}

// Result returns the varopt sketch resulting from the union of any input sketches.
func (u *VarOptItemsUnion[T]) Result() (*VarOptItemsSketch[T], error) {
	if u.gadget.numMarksInH == 0 {
		// If no marked items in H, gadget is already valid mathematically. We can return what is
		// basically just a copy of the gadget.
		return u.simpleGadgetCoercer()
	}

	// At this point, we know that marked items are present in H. So:
	//   1. Result will necessarily be in estimation mode
	//   2. Marked items currently in H need to be absorbed into reservoir (R)

	isGadgetExactMode := u.gadget.r == 0
	isGadgetPseudoExactMode := u.gadget.numMarksInH > 0

	// if gadget is pseudo-exact and the number of marks equals outerTauDenom, then we can deduce
	// from the bookkeeping logic of mergeInto() that all estimation mode input sketches must
	// have had the same tau, so we can throw all of the marked items into a common reservoir.
	allMarkedSamplesAtOuterTau := int64(u.gadget.numMarksInH) == u.outerTauDenom
	if !(isGadgetExactMode && isGadgetPseudoExactMode && allMarkedSamplesAtOuterTau) {
		return u.migrateMarkedItemsByDecreasingK()
	}

	// explicitly enforce rule that items in H should not be lighter than the sketch's tau
	for i := 0; i < u.gadget.h; i++ {
		if u.gadget.weights[i] < u.gadget.tau() && !u.gadget.marks[i] {
			return u.migrateMarkedItemsByDecreasingK()
		}
	}

	return u.markMovingGadgetCoercer()
}

func (u *VarOptItemsUnion[T]) simpleGadgetCoercer() (*VarOptItemsSketch[T], error) {
	if !((u.gadget.r == 0 && len(u.gadget.data) == u.gadget.h) || (u.gadget.r > 0 && len(u.gadget.data) == u.gadget.k+1)) {
		return nil, fmt.Errorf(
			"invalid gadget data length: got %d, h=%d, r=%d, k=%d",
			len(u.gadget.data), u.gadget.h, u.gadget.r, u.gadget.k,
		)
	}
	if cap(u.gadget.data) < len(u.gadget.data) {
		return nil, fmt.Errorf(
			"invalid gadget data capacity: cap=%d less than len=%d",
			cap(u.gadget.data), len(u.gadget.data),
		)
	}

	data := make([]T, len(u.gadget.data), cap(u.gadget.data))
	copy(data, u.gadget.data)
	weights := make([]float64, len(u.gadget.weights), cap(u.gadget.weights))
	copy(weights, u.gadget.weights)

	return &VarOptItemsSketch[T]{
		data:         data,
		weights:      weights,
		k:            u.gadget.k,
		n:            u.n,
		rf:           u.gadget.rf,
		h:            u.gadget.h,
		r:            u.gadget.r,
		totalWeightR: u.gadget.totalWeightR,
		numMarksInH:  0,
		marks:        nil,
	}, nil
}

func (u *VarOptItemsUnion[T]) migrateMarkedItemsByDecreasingK() (*VarOptItemsSketch[T], error) {
	if u.gadget.k < 2 {
		return nil, errors.New("k must be greater than 2")
	}
	if !((u.gadget.r == 0 && len(u.gadget.data) == u.gadget.h) || (u.gadget.r > 0 && len(u.gadget.data) == u.gadget.k+1)) {
		return nil, fmt.Errorf(
			"invalid gadget data length: got %d, h=%d, r=%d, k=%d",
			len(u.gadget.data), u.gadget.h, u.gadget.r, u.gadget.k,
		)
	}
	if cap(u.gadget.data) < len(u.gadget.data) {
		return nil, fmt.Errorf(
			"invalid gadget data capacity: cap=%d less than len=%d",
			cap(u.gadget.data), len(u.gadget.data),
		)
	}
	if !(u.gadget.r == 0 || u.gadget.k == (u.gadget.h+u.gadget.r)) {
		return nil, fmt.Errorf(
			"full or in pseudo-exact mode: got r=%d, k=%d, h=%d",
			u.gadget.r, u.gadget.k, u.gadget.h,
		)
	}

	data := make([]T, len(u.gadget.data), cap(u.gadget.data))
	copy(data, u.gadget.data)
	weights := make([]float64, len(u.gadget.weights), cap(u.gadget.weights))
	copy(weights, u.gadget.weights)
	var marks []bool
	if u.gadget.marks != nil {
		marks = make([]bool, len(u.gadget.marks), cap(u.gadget.marks))
		copy(marks, u.gadget.marks)
	}

	sketch := &VarOptItemsSketch[T]{
		data:         data,
		weights:      weights,
		k:            u.gadget.k,
		n:            u.n,
		rf:           u.gadget.rf,
		h:            u.gadget.h,
		r:            u.gadget.r,
		totalWeightR: u.gadget.totalWeightR,
		numMarksInH:  u.gadget.numMarksInH,
		marks:        marks,
	}
	// if non-full and pseudo-exact, change k to make full
	if sketch.r == 0 && sketch.h < sketch.k {
		sketch.k = sketch.h
	}

	if sketch.k < 2 {
		// Now k equals the number of samples, so reducing k will increase tau.
		// Also, we know that there are at least 2 samples because 0 or 1 would have been handled
		// by the earlier logic in Result()
		return nil, errors.New("k must be greater than 2")
	}
	if err := sketch.decreaseKBy1(); err != nil {
		return nil, err
	}

	// check now is in estimation mode, just like the final result must be (due to marked items)
	if sketch.r == 0 {
		return nil, fmt.Errorf(
			"invalid state after decreasing k: expected estimation mode with r > 0, got r=%d, h=%d, k=%d",
			sketch.r, sketch.h, sketch.k,
		)
	}
	if sketch.tau() == 0.0 {
		return nil, fmt.Errorf(
			"invalid state after decreasing k: expected positive tau, got tau=%g, totalWeightR=%g, r=%d",
			sketch.tau(), sketch.totalWeightR, sketch.r,
		)
	}

	// keep reducing k until all marked items have been absorbed into the reservoir
	for sketch.numMarksInH > 0 {
		if err := sketch.decreaseKBy1(); err != nil {
			return nil, err
		}
	}

	// strip marks.
	sketch.numMarksInH = 0
	sketch.marks = nil

	return sketch, nil
}

// markMovingGadgetCoercer directly transfers marked items from the gadget's H into the result's R.
// Deciding whether that is a valid thing to do is the responsibility of the caller. Currently,
// this is only used for a subcase of pseudo-exact, but later it might be used by other
// subcases as well.
func (u *VarOptItemsUnion[T]) markMovingGadgetCoercer() (*VarOptItemsSketch[T], error) {
	var (
		resultK  = u.gadget.h + u.gadget.r
		resultH  = 0
		resultR  = 0
		nextRPos = resultK // = (resultK+1)-1, to fill R region from back to front
	)

	data := make([]T, resultK+1)
	weights := make([]float64, resultK+1)
	for sample := range u.gadget.rRegionSamples() {
		data[nextRPos] = sample.Item
		weights[nextRPos] = -1.0

		resultR++
		nextRPos--
	}

	transferredWeight := 0.0
	for sample, mark := range u.gadget.hRegionSamplesWithMark() {
		if mark != nil && *mark {
			data[nextRPos] = sample.Item
			weights[nextRPos] = -1.0
			transferredWeight += sample.Weight
			resultR++
			nextRPos--
		} else {
			data[resultH] = sample.Item
			weights[resultH] = sample.Weight
			resultH++
		}
	}

	if resultH+resultR != resultK {
		return nil, fmt.Errorf(
			"invalid state after mark moving: expected %d items in H and R, got %d",
			resultK, resultH+resultR,
		)
	}
	if math.Abs(transferredWeight-u.outerTauNumer) >= 1e-10 {
		return nil, fmt.Errorf(
			"invalid state after mark moving: expected transferredWeight=%g, outerTauNumer=%g",
			transferredWeight, u.outerTauNumer,
		)
	}

	resultRWeight := u.gadget.totalWeightR + transferredWeight
	resultN := u.n

	// explicitly set values for the gap
	var zero T
	data[resultH] = zero
	weights[resultH] = -1.0
	sketch := &VarOptItemsSketch[T]{
		data:         data,
		weights:      weights,
		k:            resultK,
		n:            resultN,
		rf:           defaultResizeFactor,
		h:            resultH,
		r:            resultR,
		totalWeightR: resultRWeight,
	}
	if err := sketch.heapify(); err != nil {
		return nil, err
	}
	return sketch, nil
}
