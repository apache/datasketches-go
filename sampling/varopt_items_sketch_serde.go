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

	"github.com/apache/datasketches-go/internal"
)

const (
	varOptPreambleLongsEmpty  = 1
	varOptPreambleLongsWarmup = 3
	varOptPreambleLongsFull   = 4

	varOptSerVer     = 2
	varOptFlagEmpty  = 0x04
	varOptFlagGadget = 0x80
)

// ToSlice serializes the sketch to a byte slice using Java/C++ compatible preamble layout.
func (s *VarOptItemsSketch[T]) ToSlice(serde ItemsSerDe[T]) ([]byte, error) {
	rfBits, err := resizeFactorBitsFor(s.rf)
	if err != nil {
		return nil, err
	}

	flags := byte(0)
	if s.marks != nil {
		flags |= varOptFlagGadget
	}

	preLongs := varOptPreambleLongsEmpty
	totalItems := 0
	if s.IsEmpty() {
		flags |= varOptFlagEmpty
	} else {
		totalItems = s.h + s.r
		if s.r == 0 {
			preLongs = varOptPreambleLongsWarmup
		} else {
			preLongs = varOptPreambleLongsFull
		}
	}

	weightsBytes := s.h * 8
	markBytes := 0
	if s.marks != nil {
		markBytes = packedBoolBytes(s.h)
	}

	var items []T
	if totalItems > 0 {
		items = make([]T, 0, totalItems)
		for i := 0; i < s.h; i++ {
			items = append(items, s.data[i])
		}
		for i := s.h + 1; i <= s.k && s.r > 0; i++ {
			items = append(items, s.data[i])
		}
	}

	itemsBytes := []byte(nil)
	if totalItems > 0 {
		itemsBytes, err = serde.SerializeToBytes(items)
		if err != nil {
			return nil, err
		}
	}

	preambleBytes := preLongs * 8
	out := make([]byte, preambleBytes+weightsBytes+markBytes+len(itemsBytes))

	out[0] = rfBits | byte(preLongs)
	out[1] = varOptSerVer
	out[2] = byte(internal.FamilyEnum.VarOptItems.Id)
	out[3] = flags
	binary.LittleEndian.PutUint32(out[4:], uint32(s.k))

	if !s.IsEmpty() {
		binary.LittleEndian.PutUint64(out[8:], uint64(s.n))
		binary.LittleEndian.PutUint32(out[16:], uint32(s.h))
		binary.LittleEndian.PutUint32(out[20:], uint32(s.r))
		if s.r > 0 {
			binary.LittleEndian.PutUint64(out[24:], math.Float64bits(s.totalWeightR))
		}
	}

	weightOffset := preambleBytes
	if !s.IsEmpty() {
		weightOffset = 24
		if s.r > 0 {
			weightOffset += 8
		}
	}
	for i := 0; i < s.h; i++ {
		binary.LittleEndian.PutUint64(out[weightOffset+i*8:], math.Float64bits(s.weights[i]))
	}

	markOffset := weightOffset + weightsBytes
	if s.marks != nil && s.h > 0 {
		packBoolsInto(out[markOffset:markOffset+markBytes], s.marks[:s.h])
	}

	if totalItems > 0 {
		copy(out[markOffset+markBytes:], itemsBytes)
	}
	return out, nil
}

// NewVarOptItemsSketchFromSlice deserializes a sketch from bytes.
func NewVarOptItemsSketchFromSlice[T any](data []byte, serde ItemsSerDe[T]) (*VarOptItemsSketch[T], error) {
	if len(data) < 8 {
		return nil, errors.New("data too short")
	}

	preLongs := int(data[0] & 0x3F)
	rf, err := resizeFactorFromHeaderByte(data[0])
	if err != nil {
		return nil, err
	}
	ver := data[1]
	family := data[2]
	flags := data[3]
	k := int(binary.LittleEndian.Uint32(data[4:]))

	if ver != varOptSerVer {
		return nil, errors.New("unsupported serialization version")
	}
	if family != byte(internal.FamilyEnum.VarOptItems.Id) {
		return nil, errors.New("wrong sketch family")
	}
	if k < 1 || k > varOptMaxK {
		return nil, errors.New("invalid k in serialized varopt sketch")
	}

	hasEmptyFlag := (flags & varOptFlagEmpty) != 0
	if preLongs == varOptPreambleLongsEmpty && !hasEmptyFlag {
		return nil, errors.New("invalid varopt sketch header: empty preLongs without empty flag")
	}
	if preLongs != varOptPreambleLongsEmpty && hasEmptyFlag {
		return nil, errors.New("invalid varopt sketch header: non-empty preLongs with empty flag")
	}

	isEmpty := hasEmptyFlag
	isGadget := (flags & varOptFlagGadget) != 0
	if isEmpty {
		out, err := NewVarOptItemsSketch[T](uint(k), WithResizeFactor(rf))
		if err != nil {
			return nil, err
		}
		if isGadget {
			out.marks = make([]bool, 0, cap(out.data))
		}
		return out, nil
	}

	if preLongs != varOptPreambleLongsWarmup && preLongs != varOptPreambleLongsFull {
		return nil, errors.New("invalid preLongs for non-empty varopt sketch")
	}
	if len(data) < preLongs*8 {
		return nil, errors.New("data too short for varopt preamble")
	}

	n := int64(binary.LittleEndian.Uint64(data[8:]))
	h := int(binary.LittleEndian.Uint32(data[16:]))
	r := int(binary.LittleEndian.Uint32(data[20:]))
	if h < 0 || r < 0 {
		return nil, errors.New("invalid h/r in serialized varopt sketch")
	}
	if preLongs == varOptPreambleLongsFull && r == 0 {
		return nil, errors.New("invalid varopt sketch header: full preLongs with empty r region")
	}
	if r > 0 && h+r != k {
		return nil, errors.New("invalid varopt sketch state: h + r must equal k in sampling mode")
	}
	if r == 0 && h > k {
		return nil, errors.New("invalid varopt sketch state: h exceeds k in warmup mode")
	}

	totalWeightR := 0.0
	if r > 0 {
		totalWeightR = math.Float64frombits(binary.LittleEndian.Uint64(data[24:]))
		if math.IsNaN(totalWeightR) {
			return nil, errors.New("invalid totalWeightR in serialized varopt sketch: NaN")
		}
	}

	weightOffset := 24
	if r > 0 {
		weightOffset += 8
	}
	weightsBytes := h * 8
	if len(data) < weightOffset+weightsBytes {
		return nil, errors.New("data too short for varopt weights")
	}

	hWeights := make([]float64, h)
	for i := 0; i < h; i++ {
		w := math.Float64frombits(binary.LittleEndian.Uint64(data[weightOffset+i*8:]))
		if w <= 0 || math.IsNaN(w) || math.IsInf(w, 0) {
			return nil, errors.New("invalid non-positive or non-finite weight in serialized varopt sketch")
		}
		hWeights[i] = w
	}

	markOffset := weightOffset + weightsBytes
	hMarks := make([]bool, h)
	numMarksInH := uint32(0)
	if isGadget && h > 0 {
		markBytes := packedBoolBytes(h)
		if len(data) < markOffset+markBytes {
			return nil, errors.New("data too short for varopt marks")
		}
		unpackBoolsFrom(data[markOffset:markOffset+markBytes], hMarks)
		for _, m := range hMarks {
			if m {
				numMarksInH++
			}
		}
		markOffset += markBytes
	}

	totalItems := h + r
	items, err := serde.DeserializeFromBytes(data[markOffset:], totalItems)
	if err != nil {
		return nil, err
	}

	if r == 0 {
		out := &VarOptItemsSketch[T]{
			data:         append([]T(nil), items...),
			weights:      append([]float64(nil), hWeights...),
			k:            k,
			n:            n,
			h:            h,
			m:            0,
			r:            0,
			totalWeightR: 0,
			rf:           rf,
			numMarksInH:  numMarksInH,
		}
		if isGadget {
			out.marks = append([]bool(nil), hMarks...)
		}
		return out, nil
	}

	// Sampling mode layout uses an explicit gap slot at index h.
	dataOut := make([]T, k+1)
	weightsOut := make([]float64, k+1)
	marksOut := make([]bool, k+1)

	copy(dataOut[:h], items[:h])
	copy(dataOut[h+1:h+1+r], items[h:])
	copy(weightsOut[:h], hWeights)
	for i := h; i <= k; i++ {
		weightsOut[i] = -1.0
	}
	if isGadget {
		copy(marksOut[:h], hMarks)
	}

	out := &VarOptItemsSketch[T]{
		data:         dataOut,
		weights:      weightsOut,
		k:            k,
		n:            n,
		h:            h,
		m:            0,
		r:            r,
		totalWeightR: totalWeightR,
		rf:           rf,
		numMarksInH:  numMarksInH,
	}
	if isGadget {
		out.marks = marksOut
	}
	return out, nil
}

func packedBoolBytes(n int) int {
	if n <= 0 {
		return 0
	}
	return (n + 7) / 8
}

func packBoolsInto(dst []byte, src []bool) {
	for i, b := range src {
		if b {
			dst[i/8] |= 1 << uint(i%8)
		}
	}
}

func unpackBoolsFrom(src []byte, dst []bool) {
	for i := range dst {
		dst[i] = (src[i/8] & (1 << uint(i%8))) != 0
	}
}
