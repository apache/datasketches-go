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
	"fmt"
	"math/rand"
	"strings"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

// ResizeFactor controls how the internal array grows.
// Note: Go's slice append has automatic resizing, so this is kept for
// API compatibility with the Java version. Can be removed if not needed.
// TODO: In Java, this is abstracted into a common package. Consider if this should be moved to a common package in the future.
type ResizeFactor int

const (
	ResizeX1 ResizeFactor = 1
	ResizeX2 ResizeFactor = 2
	ResizeX4 ResizeFactor = 4
	ResizeX8 ResizeFactor = 8

	defaultResizeFactor = ResizeX8
	minK                = 2
	maxItemsSeen        = int64(0xFFFFFFFFFFFF)

	// smallest sampling array allocated: 16
	minLgArrItems = 4
)

func resizeFactorLg(rf ResizeFactor) (int, error) {
	switch rf {
	case ResizeX1:
		return 0, nil
	case ResizeX2:
		return 1, nil
	case ResizeX4:
		return 2, nil
	case ResizeX8:
		return 3, nil
	default:
		return 0, errors.New("unsupported resize factor")
	}
}

func mustResizeFactorLg(rf ResizeFactor) int {
	lgRf, err := resizeFactorLg(rf)
	if err != nil {
		panic(err)
	}
	return lgRf
}

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
	rf   ResizeFactor
	data []T // reservoir storage
}

type reservoirItemsSketchOptions struct {
	resizeFactor ResizeFactor
}

// ReservoirItemsSketchOptionFunc defines a functional option for configuring reservoirItemsSketchOptions.
type ReservoirItemsSketchOptionFunc func(*reservoirItemsSketchOptions)

// WithReservoirItemsSketchResizeFactor sets the resize factor for the internal array.
func WithReservoirItemsSketchResizeFactor(rf ResizeFactor) ReservoirItemsSketchOptionFunc {
	return func(r *reservoirItemsSketchOptions) {
		r.resizeFactor = rf
	}
}

// NewReservoirItemsSketch creates a new reservoir sketch with the given capacity k.
func NewReservoirItemsSketch[T any](
	k int, opts ...ReservoirItemsSketchOptionFunc,
) (*ReservoirItemsSketch[T], error) {
	if k < minK {
		return nil, errors.New("k must be at least 2")
	}

	options := &reservoirItemsSketchOptions{
		resizeFactor: defaultResizeFactor,
	}
	for _, opt := range opts {
		opt(options)
	}

	lgRf, err := resizeFactorLg(options.resizeFactor)
	if err != nil {
		return nil, err
	}

	ceilingLgK, _ := internal.ExactLog2(common.CeilingPowerOf2(k))
	initialLgSize := startingSubMultiple(
		ceilingLgK, lgRf, minLgArrItems,
	)
	return &ReservoirItemsSketch[T]{
		k:    k,
		n:    0,
		rf:   options.resizeFactor,
		data: make([]T, 0, adjustedSamplingAllocationSize(k, 1<<initialLgSize)),
	}, nil
}

// Update adds an item to the sketch using reservoir sampling algorithm.
func (s *ReservoirItemsSketch[T]) Update(item T) {
	if s.n == maxItemsSeen {
		panic(fmt.Sprintf("sketch has exceeded capacity for total items seen: %d", maxItemsSeen))
	}

	if s.n < int64(s.k) {
		// Initial phase: store all items until reservoir is full
		if s.n >= int64(cap(s.data)) {
			s.growReservoir()
		}

		s.data = append(s.data, item)
		s.n++
	} else {
		// Steady state: replace with probability k/n
		s.n++
		if rand.Float64()*float64(s.n) < float64(s.k) {
			s.data[rand.Intn(s.k)] = item
		}
	}
}

func (s *ReservoirItemsSketch[T]) growReservoir() {
	lgRf := mustResizeFactorLg(s.rf)
	targetCap := adjustedSamplingAllocationSize(s.k, cap(s.data)<<lgRf)
	if targetCap <= cap(s.data) {
		return
	}
	newData := make([]T, len(s.data), targetCap)
	copy(newData, s.data)
	s.data = newData
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
	lgRf := mustResizeFactorLg(s.rf)
	ceilingLgK, _ := internal.ExactLog2(common.CeilingPowerOf2(s.k))
	initialLgSize := startingSubMultiple(
		ceilingLgK, lgRf, minLgArrItems,
	)

	s.n = 0
	s.data = make([]T, 0, adjustedSamplingAllocationSize(s.k, 1<<initialLgSize))
}

// ImplicitSampleWeight returns N/K when in sampling mode, or 1.0 in exact mode.
func (s *ReservoirItemsSketch[T]) ImplicitSampleWeight() float64 {
	if s.n < int64(s.k) {
		return 1.0
	}
	return float64(s.n) / float64(s.k)
}

// Copy returns a deep copy of the sketch.
func (s *ReservoirItemsSketch[T]) Copy() *ReservoirItemsSketch[T] {
	dataCopy := make([]T, len(s.data))
	copy(dataCopy, s.data)
	return &ReservoirItemsSketch[T]{
		k:    s.k,
		n:    s.n,
		rf:   s.rf,
		data: dataCopy,
	}
}

// EstimateSubsetSum computes an estimated subset sum from the entire stream for objects matching a given
// predicate. Provides a lower bound, estimate, and upper bound using a target of 2 standard deviations.
//
// NOTE: This is technically a heuristic method, and tries to err on the conservative side.
//
// predicate: A predicate to use when identifying items.
// Returns a summary object containing the estimate, upper and lower bounds, and the total sketch weight.
func (s *ReservoirItemsSketch[T]) EstimateSubsetSum(predicate func(T) bool) (SampleSubsetSummary, error) {
	if s.n == 0 {
		return SampleSubsetSummary{}, nil
	}

	numSamples := s.NumSamples()
	samplingRate := float64(numSamples) / float64(s.n)

	trueCount := 0
	for _, sample := range s.data {
		if predicate(sample) {
			trueCount++
		}
	}

	if s.n <= int64(s.k) { // exact mode.
		return SampleSubsetSummary{
			LowerBound:        float64(trueCount),
			Estimate:          float64(trueCount),
			UpperBound:        float64(trueCount),
			TotalSketchWeight: float64(numSamples),
		}, nil
	}

	lowerBoundTrueFraction, err := pseudoHypergeometricLowerBoundOnP(uint64(numSamples), uint64(trueCount), samplingRate)
	if err != nil {
		return SampleSubsetSummary{}, err
	}
	upperBoundTrueFraction, err := pseudoHypergeometricUpperBoundOnP(uint64(numSamples), uint64(trueCount), samplingRate)
	if err != nil {
		return SampleSubsetSummary{}, err
	}
	estimatedTrueFraction := (1.0 * float64(trueCount)) / float64(numSamples)
	return SampleSubsetSummary{
		LowerBound:        float64(s.n) * lowerBoundTrueFraction,
		Estimate:          float64(s.n) * estimatedTrueFraction,
		UpperBound:        float64(s.n) * upperBoundTrueFraction,
		TotalSketchWeight: float64(s.n),
	}, nil
}

// DownsampledCopy returns a copy with a reduced reservoir size.
// If newK >= current K, returns a regular copy.
func (s *ReservoirItemsSketch[T]) DownsampledCopy(newK int) (*ReservoirItemsSketch[T], error) {
	if newK >= s.k {
		return s.Copy(), nil
	}

	result, err := NewReservoirItemsSketch[T](newK, WithReservoirItemsSketchResizeFactor(s.rf))
	if err != nil {
		return nil, err
	}

	samples := s.Samples()
	for _, item := range samples {
		result.Update(item)
	}

	// Adjust N to preserve correct implicit weights
	if result.n < s.n {
		result.forceIncrementItemsSeen(s.n - result.n)
	}

	return result, nil
}

// valueAtPosition returns the item at the given position.
func (s *ReservoirItemsSketch[T]) valueAtPosition(pos int) T {
	return s.data[pos]
}

// insertValueAtPosition replaces the item at the given position.
func (s *ReservoirItemsSketch[T]) insertValueAtPosition(item T, pos int) {
	s.data[pos] = item
}

// forceIncrementItemsSeen adds delta to the items seen count.
func (s *ReservoirItemsSketch[T]) forceIncrementItemsSeen(delta int64) {
	s.n += delta
	if s.n > maxItemsSeen {
		panic(fmt.Sprintf(
			"sketch has exceeded capacity for total items seen. limit: %d, found: %d",
			maxItemsSeen, s.n,
		))
	}
}

// Serialization constants
const (
	preambleIntsEmpty    = 1
	preambleIntsNonEmpty = 2
	serVer               = 2
	flagEmpty            = 0x04
	resizeFactorMask     = 0xC0
)

func resizeFactorBitsFor(rf ResizeFactor) (byte, error) {
	switch rf {
	case ResizeX1:
		return 0x00, nil
	case ResizeX2:
		return 0x40, nil
	case ResizeX4:
		return 0x80, nil
	case ResizeX8:
		return 0xC0, nil
	default:
		return 0, errors.New("unsupported resize factor")
	}
}

func resizeFactorFromHeaderByte(b byte) (ResizeFactor, error) {
	switch (b & resizeFactorMask) >> 6 {
	case 0:
		return ResizeX1, nil
	case 1:
		return ResizeX2, nil
	case 2:
		return ResizeX4, nil
	case 3:
		return ResizeX8, nil
	default:
		return 0, errors.New("unsupported resize factor bits")
	}
}

// ToSlice serializes the sketch to a byte slice.
func (s *ReservoirItemsSketch[T]) ToSlice(serde ItemsSerDe[T]) ([]byte, error) {
	rfBits, err := resizeFactorBitsFor(s.rf)
	if err != nil {
		return nil, err
	}

	if s.IsEmpty() {
		buf := make([]byte, 8)
		buf[0] = rfBits | preambleIntsEmpty
		buf[1] = serVer
		buf[2] = byte(internal.FamilyEnum.ReservoirItems.Id)
		buf[3] = flagEmpty
		binary.LittleEndian.PutUint32(buf[4:], uint32(s.k))
		return buf, nil
	}

	itemsBytes, err := serde.SerializeToBytes(s.data)
	if err != nil {
		return nil, err
	}

	preambleBytes := preambleIntsNonEmpty * 8
	buf := make([]byte, preambleBytes+len(itemsBytes))

	buf[0] = rfBits | preambleIntsNonEmpty
	buf[1] = serVer
	buf[2] = byte(internal.FamilyEnum.ReservoirItems.Id)
	buf[3] = 0
	binary.LittleEndian.PutUint32(buf[4:], uint32(s.k))
	binary.LittleEndian.PutUint64(buf[8:], uint64(s.n))

	copy(buf[preambleBytes:], itemsBytes)

	return buf, nil
}

// String returns human-readable summary of the sketch, without items.
func (s *ReservoirItemsSketch[T]) String() string {
	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString("### ")
	sb.WriteString("ReservoirItemsSketch")
	sb.WriteString(" SUMMARY: \n")
	sb.WriteString("   k            : ")
	sb.WriteString(fmt.Sprintf("%d", s.k))
	sb.WriteString("\n")
	sb.WriteString("   n            : ")
	sb.WriteString(fmt.Sprintf("%d", s.n))
	sb.WriteString("\n")
	sb.WriteString("   Current size : ")
	sb.WriteString(fmt.Sprintf("%d", len(s.data)))
	sb.WriteString("\n")
	sb.WriteString("   Resize factor: ")
	sb.WriteString(fmt.Sprintf("%d", s.rf))
	sb.WriteString("\n")
	sb.WriteString("### END SKETCH SUMMARY\n")

	return sb.String()
}

// NewReservoirItemsSketchFromSlice deserializes a sketch from a byte slice.
func NewReservoirItemsSketchFromSlice[T any](data []byte, serde ItemsSerDe[T]) (*ReservoirItemsSketch[T], error) {
	if len(data) < 8 {
		return nil, errors.New("data too short")
	}

	preambleInts := int(data[0] & 0x3F)
	ver := data[1]
	family := data[2]
	flags := data[3]
	k := int(binary.LittleEndian.Uint32(data[4:]))
	rf, err := resizeFactorFromHeaderByte(data[0])
	if err != nil {
		return nil, err
	}

	if ver != serVer {
		if ver == 1 {
			encK := binary.LittleEndian.Uint16(data[4:])
			decodedK, err := decodeReservoirSize(encK)
			if err != nil {
				return nil, err
			}
			k = decodedK
		} else {
			return nil, errors.New("unsupported serialization version")
		}
	}
	if family != byte(internal.FamilyEnum.ReservoirItems.Id) {
		return nil, errors.New("wrong sketch family")
	}

	if (flags&flagEmpty) != 0 || preambleInts == preambleIntsEmpty {
		return NewReservoirItemsSketch[T](k, WithReservoirItemsSketchResizeFactor(rf))
	}

	preambleBytes := preambleIntsNonEmpty * 8
	if len(data) < preambleBytes {
		return nil, errors.New("data too short for non-empty sketch")
	}

	n := int64(binary.LittleEndian.Uint64(data[8:]))
	if n > maxItemsSeen {
		return nil, fmt.Errorf("items seen exceeds limit: %d", maxItemsSeen)
	}
	numSamples := int(min(n, int64(k)))

	itemsData := data[preambleBytes:]

	items, err := serde.DeserializeFromBytes(itemsData, numSamples)
	if err != nil {
		return nil, err
	}

	return &ReservoirItemsSketch[T]{
		k:    k,
		n:    n,
		rf:   rf,
		data: items,
	}, nil
}
