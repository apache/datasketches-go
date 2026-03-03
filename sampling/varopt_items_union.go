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
	"math"
	"math/rand"

	"github.com/apache/datasketches-go/internal"
)

// VarOptItemsUnion provides union operations over VarOpt sketches.
//
// This file implements the phase-1 core API used by callers to merge sketches.
// Full gadget-resolution logic for marked items in H is handled in a follow-up step.
type VarOptItemsUnion[T any] struct {
	gadget *VarOptItemsSketch[T]
	maxK   int

	n int64

	// outer tau is the largest tau of any input sketch in estimation mode
	outerTauNumer float64
	outerTauDenom int64
}

func NewVarOptItemsUnion[T any](maxK int) (*VarOptItemsUnion[T], error) {
	gadget, err := newVarOptItemsSketchAsGadget[T](maxK)
	if err != nil {
		return nil, err
	}

	return &VarOptItemsUnion[T]{
		gadget: gadget,
		maxK:   maxK,
	}, nil
}

func (u *VarOptItemsUnion[T]) Reset() error {
	if u.gadget == nil {
		gadget, err := newVarOptItemsSketchAsGadget[T](u.maxK)
		if err != nil {
			return err
		}
		u.gadget = gadget
	} else {
		u.gadget.Reset()
	}

	u.n = 0
	u.outerTauNumer = 0
	u.outerTauDenom = 0
	return nil
}

// UpdateSketch merges a VarOpt sketch into this union.
func (u *VarOptItemsUnion[T]) UpdateSketch(sketch *VarOptItemsSketch[T]) error {
	if sketch == nil || sketch.N() == 0 {
		return nil
	}

	u.n += sketch.N()

	// Insert H-region items as unmarked.
	for i := 0; i < sketch.h; i++ {
		if err := u.gadget.update(sketch.data[i], sketch.weights[i], false); err != nil {
			return err
		}
	}

	// Insert R-region items as marked with corrected weight tau.
	if sketch.r > 0 {
		tau := sketch.tau()
		cumWeight := 0.0
		rSeen := 0
		for i := sketch.h + 1; i <= sketch.k; i++ {
			w := tau
			// Match Java/C++ weight-correcting iterator semantics:
			// correct the last R item to absorb floating-point residual.
			if rSeen == sketch.r-1 {
				w = sketch.totalWeightR - cumWeight
			} else {
				cumWeight += tau
			}
			rSeen++
			if err := u.gadget.update(sketch.data[i], w, true); err != nil {
				return err
			}
		}
		u.resolveOuterTau(sketch)
	}

	return nil
}

// Result returns the current union result sketch.
//
// If marked items remain in H, full resolution logic is required and is implemented
// in the next step. For now we fail fast with a clear error.
func (u *VarOptItemsUnion[T]) Result() (*VarOptItemsSketch[T], error) {
	if u.gadget == nil || u.gadget.N() == 0 {
		return NewVarOptItemsSketch[T](uint(u.maxK))
	}

	if u.gadget.numMarksInH == 0 {
		out := copyVarOptItemsSketch(u.gadget, true)
		out.n = u.n
		return out, nil
	}

	// Marked items in H require the full resolution path.
	if out, ok, err := u.detectAndHandleSubcaseOfPseudoExact(); err != nil {
		return nil, err
	} else if ok {
		return out, nil
	}
	return u.migrateMarkedItemsByDecreasingK()
}

func (u *VarOptItemsUnion[T]) resolveOuterTau(sketch *VarOptItemsSketch[T]) {
	if sketch.r == 0 {
		return
	}

	sketchTau := sketch.tau()
	if u.outerTauDenom == 0 {
		u.outerTauNumer = sketch.totalWeightR
		u.outerTauDenom = int64(sketch.r)
		return
	}

	outerTau := u.outerTauNumer / float64(u.outerTauDenom)
	if sketchTau > outerTau {
		u.outerTauNumer = sketch.totalWeightR
		u.outerTauDenom = int64(sketch.r)
		return
	}
	if sketchTau == outerTau {
		u.outerTauNumer += sketch.totalWeightR
		u.outerTauDenom += int64(sketch.r)
	}
}

func newVarOptItemsSketchAsGadget[T any](k int) (*VarOptItemsSketch[T], error) {
	sketch, err := NewVarOptItemsSketch[T](uint(k))
	if err != nil {
		return nil, err
	}
	sketch.marks = make([]bool, 0, cap(sketch.data))
	return sketch, nil
}

func (u *VarOptItemsUnion[T]) detectAndHandleSubcaseOfPseudoExact() (*VarOptItemsSketch[T], bool, error) {
	condition1 := u.gadget.r == 0
	condition2 := u.gadget.numMarksInH > 0
	condition3 := int64(u.gadget.numMarksInH) == u.outerTauDenom

	if !(condition1 && condition2 && condition3) {
		return nil, false, nil
	}

	if u.thereExistUnmarkedHItemsLighterThanTarget(u.gadget.tau()) {
		return nil, false, nil
	}

	out, err := u.markMovingGadgetCoercer()
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (u *VarOptItemsUnion[T]) thereExistUnmarkedHItemsLighterThanTarget(threshold float64) bool {
	for i := 0; i < u.gadget.h; i++ {
		if u.gadget.weights[i] < threshold && !u.gadget.marks[i] {
			return true
		}
	}
	return false
}

func (u *VarOptItemsUnion[T]) markMovingGadgetCoercer() (*VarOptItemsSketch[T], error) {
	resultK := u.gadget.h + u.gadget.r
	resultH := 0
	resultR := 0
	nextRPos := resultK

	data := make([]T, resultK+1)
	weights := make([]float64, resultK+1)

	// Move existing R region items first (weight remains implicit via totalWeightR).
	for i := u.gadget.h + 1; i <= u.gadget.k && i < len(u.gadget.data); i++ {
		data[nextRPos] = u.gadget.data[i]
		weights[nextRPos] = -1.0
		resultR++
		nextRPos--
	}

	transferredWeight := 0.0
	for i := 0; i < u.gadget.h; i++ {
		if u.gadget.marks[i] {
			data[nextRPos] = u.gadget.data[i]
			weights[nextRPos] = -1.0
			transferredWeight += u.gadget.weights[i]
			resultR++
			nextRPos--
		} else {
			data[resultH] = u.gadget.data[i]
			weights[resultH] = u.gadget.weights[i]
			resultH++
		}
	}

	if resultH+resultR != resultK {
		return nil, errors.New("invalid state resolving pseudo-exact union gadget")
	}
	if math.Abs(transferredWeight-u.outerTauNumer) > 1e-10 {
		return nil, errors.New("unexpected mismatch in transferred weight")
	}

	// Gap slot.
	weights[resultH] = -1.0

	out := &VarOptItemsSketch[T]{
		data:         data,
		weights:      weights,
		k:            resultK,
		n:            u.n,
		h:            resultH,
		m:            0,
		r:            resultR,
		totalWeightR: u.gadget.totalWeightR + transferredWeight,
		rf:           varOptDefaultResizeFactor,
		numMarksInH:  0,
	}

	if err := out.heapify(); err != nil {
		return nil, err
	}
	return out, nil
}

func (u *VarOptItemsUnion[T]) migrateMarkedItemsByDecreasingK() (*VarOptItemsSketch[T], error) {
	gcopy := copyVarOptItemsSketch(u.gadget, false)
	gcopy.n = u.n

	rCount := gcopy.r
	hCount := gcopy.h
	k := gcopy.k

	// If non-full and pseudo-exact, set k to sample count so reductions increase tau.
	if rCount == 0 && hCount < k {
		gcopy.k = hCount
	}

	if gcopy.k < 2 {
		return nil, errors.New("cannot resolve marked items with k < 2")
	}
	if err := decreaseKBy1(gcopy); err != nil {
		return nil, err
	}

	for gcopy.numMarksInH > 0 {
		if gcopy.k < 2 {
			return nil, errors.New("cannot continue resolving marked items with k < 2")
		}
		if err := decreaseKBy1(gcopy); err != nil {
			return nil, err
		}
	}

	gcopy.numMarksInH = 0
	gcopy.marks = nil
	return gcopy, nil
}

func decreaseKBy1[T any](s *VarOptItemsSketch[T]) error {
	if s.k <= 1 {
		return errors.New("cannot decrease k below 1 in union")
	}

	switch {
	case s.h == 0 && s.r == 0:
		s.k--
		return nil
	case s.h > 0 && s.r == 0:
		s.k--
		if s.h > s.k {
			return s.transitionFromWarmup()
		}
		return nil
	case s.h > 0 && s.r > 0:
		oldGapIdx := s.h
		oldFinalRIdx := (s.h + 1 + s.r) - 1
		s.swap(oldFinalRIdx, oldGapIdx)

		pulledIdx := s.h - 1
		pulledItem := s.data[pulledIdx]
		pulledWeight := s.weights[pulledIdx]
		pulledMark := s.marks[pulledIdx]

		if pulledMark {
			s.numMarksInH--
		}
		s.weights[pulledIdx] = -1.0

		s.h--
		s.k--
		s.n--
		return s.update(pulledItem, pulledWeight, pulledMark)
	case s.h == 0 && s.r > 0:
		if s.r < 2 {
			return errors.New("invalid pure-reservoir state while decreasing k")
		}
		rIdxToDelete := 1 + rand.Intn(s.r)
		rightmostRIdx := (1 + s.r) - 1
		s.swap(rIdxToDelete, rightmostRIdx)
		s.weights[rightmostRIdx] = -1.0

		s.k--
		s.r--
		return nil
	default:
		return errors.New("invalid sketch state while decreasing k")
	}
}

func copyVarOptItemsSketch[T any](in *VarOptItemsSketch[T], asSketch bool) *VarOptItemsSketch[T] {
	dataCopy := make([]T, len(in.data))
	copy(dataCopy, in.data)

	weightsCopy := make([]float64, len(in.weights))
	copy(weightsCopy, in.weights)

	var marksCopy []bool
	numMarksInH := in.numMarksInH
	if !asSketch && in.marks != nil {
		marksCopy = make([]bool, len(in.marks))
		copy(marksCopy, in.marks)
	} else {
		numMarksInH = 0
	}

	return &VarOptItemsSketch[T]{
		data:         dataCopy,
		weights:      weightsCopy,
		marks:        marksCopy,
		k:            in.k,
		n:            in.n,
		h:            in.h,
		m:            in.m,
		r:            in.r,
		totalWeightR: in.totalWeightR,
		rf:           in.rf,
		numMarksInH:  numMarksInH,
	}
}

// ToSlice serializes the union state to bytes.
func (u *VarOptItemsUnion[T]) ToSlice(serde ItemsSerDe[T]) ([]byte, error) {
	empty := u.gadget == nil || u.gadget.N() == 0
	if empty {
		out := make([]byte, varOptUnionPreambleLongsEmpty*8)
		out[0] = byte(varOptUnionPreambleLongsEmpty)
		out[1] = varOptUnionSerVer
		out[2] = byte(internal.FamilyEnum.VarOptUnion.Id)
		out[3] = varOptUnionFlagEmpty
		binary.LittleEndian.PutUint32(out[4:], uint32(u.maxK))
		return out, nil
	}

	gadgetBytes, err := u.gadget.ToSlice(serde)
	if err != nil {
		return nil, err
	}

	preBytes := varOptUnionPreambleLongsNonEmpty * 8
	out := make([]byte, preBytes+len(gadgetBytes))
	out[0] = byte(varOptUnionPreambleLongsNonEmpty)
	out[1] = varOptUnionSerVer
	out[2] = byte(internal.FamilyEnum.VarOptUnion.Id)
	out[3] = 0
	binary.LittleEndian.PutUint32(out[4:], uint32(u.maxK))
	binary.LittleEndian.PutUint64(out[8:], uint64(u.n))
	binary.LittleEndian.PutUint64(out[16:], math.Float64bits(u.outerTauNumer))
	binary.LittleEndian.PutUint64(out[24:], uint64(u.outerTauDenom))
	copy(out[preBytes:], gadgetBytes)
	return out, nil
}

// NewVarOptItemsUnionFromSlice deserializes union state from bytes.
func NewVarOptItemsUnionFromSlice[T any](data []byte, serde ItemsSerDe[T]) (*VarOptItemsUnion[T], error) {
	if len(data) < 8 {
		return nil, errors.New("data too short")
	}
	preLongs := int(data[0] & 0x3F)
	ver := data[1]
	family := data[2]
	flags := data[3]
	maxK := int(binary.LittleEndian.Uint32(data[4:]))

	if ver != varOptUnionSerVer {
		return nil, errors.New("unsupported serialization version")
	}
	if family != byte(internal.FamilyEnum.VarOptUnion.Id) {
		return nil, errors.New("wrong sketch family")
	}
	if preLongs != varOptUnionPreambleLongsEmpty && preLongs != varOptUnionPreambleLongsNonEmpty {
		return nil, errors.New("invalid preLongs for varopt union")
	}

	union, err := NewVarOptItemsUnion[T](maxK)
	if err != nil {
		return nil, err
	}

	hasEmptyFlag := (flags & varOptUnionFlagEmpty) != 0
	if preLongs == varOptUnionPreambleLongsEmpty && !hasEmptyFlag {
		return nil, errors.New("invalid varopt union header: empty preLongs without empty flag")
	}
	if preLongs != varOptUnionPreambleLongsEmpty && hasEmptyFlag {
		return nil, errors.New("invalid varopt union header: non-empty preLongs with empty flag")
	}

	if hasEmptyFlag {
		return union, nil
	}

	if len(data) < varOptUnionPreambleLongsNonEmpty*8 {
		return nil, errors.New("data too short for non-empty varopt union")
	}

	union.n = int64(binary.LittleEndian.Uint64(data[8:]))
	union.outerTauNumer = math.Float64frombits(binary.LittleEndian.Uint64(data[16:]))
	union.outerTauDenom = int64(binary.LittleEndian.Uint64(data[24:]))

	gadget, err := NewVarOptItemsSketchFromSlice[T](data[varOptUnionPreambleLongsNonEmpty*8:], serde)
	if err != nil {
		return nil, err
	}
	union.gadget = gadget
	return union, nil
}
