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

package tuple

import (
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/internal/binomialbounds"
	"github.com/apache/datasketches-go/theta"
)

// ArrayOfNumbersUpdateSketch builds Tuple sketch from input data via update methods.
// This is a wrapper around a tuple sketch to match the functionality
// and serialization format of ArrayOfDoublesSketch in Java.
type ArrayOfNumbersUpdateSketch[V Number] struct {
	table                   *hashtable[*ArrayOfNumbersSummary[V]]
	numberOfValuesInSummary uint8
}

// NewArrayOfNumbersUpdateSketch initializes and returns a new instance of ArrayOfNumbersUpdateSketch with the specified parameters.
func NewArrayOfNumbersUpdateSketch[V Number](
	numberOfValuesInSummary uint8, opts ...UpdateSketchOptionFunc,
) (*ArrayOfNumbersUpdateSketch[V], error) {
	options := &updateSketchOptions{
		lgK:  theta.DefaultLgK,
		rf:   theta.DefaultResizeFactor,
		p:    1.0,
		seed: theta.DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.lgK < theta.MinLgK {
		return nil, fmt.Errorf("lg_k must not be less than %d: %d", theta.MinLgK, options.lgK)
	}
	if options.lgK > theta.MaxLgK {
		return nil, fmt.Errorf("lg_k must not be greater than %d: %d", theta.MaxLgK, options.lgK)
	}
	if options.p <= 0 || options.p > 1 {
		return nil, errors.New("sampling probability must be between 0 and 1")
	}

	options.lgCurSize = startingSubMultiple(options.lgK+1, theta.MinLgK, uint8(options.rf))
	options.theta = startingThetaFromP(options.p)

	return &ArrayOfNumbersUpdateSketch[V]{
		table: newHashtable[*ArrayOfNumbersSummary[V]](
			options.lgCurSize,
			options.lgK,
			options.rf,
			options.p,
			options.theta,
			options.seed,
			true,
		),
		numberOfValuesInSummary: numberOfValuesInSummary,
	}, nil
}

// IsEstimationMode reports whether the sketch is in estimation mode,
// as opposed to exact mode.
func (s *ArrayOfNumbersUpdateSketch[V]) IsEstimationMode() bool {
	return s.Theta64() < theta.MaxTheta && !s.IsEmpty()
}

// Theta returns theta as a fraction from 0 to 1, representing the
// effective sampling rate.
func (s *ArrayOfNumbersUpdateSketch[V]) Theta() float64 {
	return float64(s.Theta64()) / float64(theta.MaxTheta)
}

// Estimate returns the estimated distinct count of the input stream.
func (s *ArrayOfNumbersUpdateSketch[V]) Estimate() float64 {
	return float64(s.NumRetained()) / s.Theta()
}

// LowerBoundFromSubset returns the approximate lower error bound for
// the given number of standard deviations over a subset of retained hashes.
// numStdDevs specifies the confidence level (1, 2, or 3) corresponding to
// approximately 67%, 95%, or 99% confidence intervals.
// numSubsetEntries specifies number of items from {0, 1, ..., get_num_retained()}
// over which to estimate the bound.
func (s *ArrayOfNumbersUpdateSketch[V]) LowerBoundFromSubset(numStdDevs uint8, numSubsetEntries uint32) (float64, error) {
	numSubsetEntries = min(numSubsetEntries, s.NumRetained())
	if !s.IsEstimationMode() {
		return float64(numSubsetEntries), nil
	}
	return binomialbounds.LowerBound(uint64(numSubsetEntries), s.Theta(), uint(numStdDevs))
}

// LowerBound returns the approximate lower error bound for the given
// number of standard deviations. numStdDevs should be 1, 2, or 3 for
// approximately 67%, 95%, or 99% confidence intervals.
func (s *ArrayOfNumbersUpdateSketch[V]) LowerBound(numStdDevs uint8) (float64, error) {
	return s.LowerBoundFromSubset(numStdDevs, s.NumRetained())
}

// UpperBoundFromSubset returns the approximate upper error bound for
// the given number of standard deviations over a subset of retained hashes.
// numStdDevs specifies the confidence level (1, 2, or 3) corresponding to
// approximately 67%, 95%, or 99% confidence intervals.
// numSubsetEntries specifies number of items from {0, 1, ..., get_num_retained()}
// over which to estimate the bound.
func (s *ArrayOfNumbersUpdateSketch[V]) UpperBoundFromSubset(numStdDevs uint8, numSubsetEntries uint32) (float64, error) {
	numSubsetEntries = min(numSubsetEntries, s.NumRetained())
	if !s.IsEstimationMode() {
		return float64(numSubsetEntries), nil
	}
	return binomialbounds.UpperBound(uint64(numSubsetEntries), s.Theta(), uint(numStdDevs))
}

// UpperBound returns the approximate upper error bound for the given
// number of standard deviations. numStdDevs should be 1, 2, or 3 for
// approximately 67%, 95%, or 99% confidence intervals.
func (s *ArrayOfNumbersUpdateSketch[V]) UpperBound(numStdDevs uint8) (float64, error) {
	return s.UpperBoundFromSubset(numStdDevs, s.NumRetained())
}

// String returns a human-readable summary of this sketch.
// If printItems is true, the output includes all retained hashes.
func (s *ArrayOfNumbersUpdateSketch[V]) String(shouldPrintItems bool) string {
	seedHash, _ := s.SeedHash()
	lb, _ := s.LowerBound(2)
	ub, _ := s.UpperBound(2)

	var result strings.Builder
	result.WriteString("### Tuple sketch summary:")
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   num retained hashes : %d", s.NumRetained()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   seed hash            : %d", seedHash))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   empty?               : %t", s.IsEmpty()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   ordered?             : %t", s.IsOrdered()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   estimation mode?     : %t", s.IsEstimationMode()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   theta (fraction)     : %f", s.Theta()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   theta (raw 64-bit)   : %d", s.Theta64()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   estimate             : %f", s.Estimate()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   lower bound 95%% conf : %f", lb))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   upper bound 95%% conf : %f", ub))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   lg nominal size      : %d", s.table.lgNomSize))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   lg current size      : %d", s.table.lgCurSize))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   resize factor        : %d", 1<<s.table.rf))
	result.WriteString("\n")
	result.WriteString("### End sketch summary")
	result.WriteString("\n")

	if shouldPrintItems {
		result.WriteString("### Retained entries")
		result.WriteString("\n")

		for hash, summary := range s.All() {
			result.WriteString(fmt.Sprintf("%d: %v", hash, summary))
			result.WriteString("\n")
		}

		result.WriteString("### End retained entries")
		result.WriteString("\n")
	}

	return result.String()
}

// IsEmpty reports whether this sketch represents an empty set.
func (s *ArrayOfNumbersUpdateSketch[V]) IsEmpty() bool {
	return s.table.isEmpty
}

// IsOrdered reports whether retained hashes are sorted by hash value.
func (s *ArrayOfNumbersUpdateSketch[V]) IsOrdered() bool {
	return s.table.numEntries <= 1
}

// Theta64 returns theta as a positive integer between 0 and math.MaxUint64.
func (s *ArrayOfNumbersUpdateSketch[V]) Theta64() uint64 {
	if s.IsEmpty() {
		return theta.MaxTheta
	}
	return s.table.theta
}

// NumRetained returns the number of hashes retained in the sketch.
func (s *ArrayOfNumbersUpdateSketch[V]) NumRetained() uint32 {
	return s.table.numEntries
}

// SeedHash returns the hash of the seed used to hash the input.
func (s *ArrayOfNumbersUpdateSketch[V]) SeedHash() (uint16, error) {
	seedHash, err := internal.ComputeSeedHash(int64(s.table.seed))
	if err != nil {
		return 0, err
	}
	return uint16(seedHash), nil
}

// All returns an iterator over all hash-summary pairs in the sketch.
func (s *ArrayOfNumbersUpdateSketch[V]) All() iter.Seq2[uint64, *ArrayOfNumbersSummary[V]] {
	return func(yield func(uint64, *ArrayOfNumbersSummary[V]) bool) {
		for _, e := range s.table.entries {
			if e.Hash != 0 {
				if !yield(e.Hash, e.Summary) {
					return
				}
			}
		}
	}
}

// LgK returns a configured nominal number of entries in the sketch
func (s *ArrayOfNumbersUpdateSketch[V]) LgK() uint8 {
	return s.table.lgNomSize
}

// ResizeFactor returns a configured resize factor of the sketch
func (s *ArrayOfNumbersUpdateSketch[V]) ResizeFactor() theta.ResizeFactor {
	return s.table.rf
}

// UpdateUint64 updates this sketch with a given unsigned 64-bit integer
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateUint64(key uint64, values []V) error {
	return s.UpdateInt64(int64(key), values)
}

// UpdateInt64 updates this sketch with a given signed 64-bit integer
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateInt64(key int64, values []V) error {
	hash, err := s.table.HashInt64AndScreen(key)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			summary := newArrayOfNumbersSummary[V](s.numberOfValuesInSummary)
			summary.Update(values)

			s.table.Insert(index, entry[*ArrayOfNumbersSummary[V]]{Hash: hash, Summary: summary})
			return nil
		}
		return err
	}

	s.table.entries[index].Summary.Update(values)
	return nil
}

// UpdateUint32 updates this sketch with a given unsigned 32-bit integer
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateUint32(key uint32, values []V) error {
	return s.UpdateInt64(int64(key), values)
}

// UpdateInt32 updates this sketch with a given signed 32-bit integer
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateInt32(key int32, values []V) error {
	hash, err := s.table.HashInt32AndScreen(key)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			summary := newArrayOfNumbersSummary[V](s.numberOfValuesInSummary)
			summary.Update(values)

			s.table.Insert(index, entry[*ArrayOfNumbersSummary[V]]{Hash: hash, Summary: summary})
			return nil
		}
		return err
	}

	s.table.entries[index].Summary.Update(values)
	return nil
}

// UpdateUint16 updates this sketch with a given unsigned 16-bit integer
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateUint16(key uint16, values []V) error {
	return s.UpdateInt32(int32(key), values)
}

// UpdateInt16 updates this sketch with a given signed 16-bit integer
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateInt16(key int16, values []V) error {
	return s.UpdateInt32(int32(key), values)
}

// UpdateUint8 updates this sketch with a given unsigned 8-bit integer
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateUint8(key uint8, values []V) error {
	return s.UpdateInt32(int32(key), values)
}

// UpdateInt8 updates this sketch with a given signed 8-bit integer
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateInt8(key int8, values []V) error {
	return s.UpdateInt32(int32(key), values)
}

// UpdateFloat64 updates this sketch with a given double-precision floating point value
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateFloat64(key float64, values []V) error {
	return s.UpdateInt64(canonicalDouble(key), values)
}

// UpdateFloat32 updates this sketch with a given floating point value
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateFloat32(key float32, values []V) error {
	return s.UpdateFloat64(float64(key), values)
}

// UpdateString updates this sketch with a given string
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateString(key string, values []V) error {
	if key == "" {
		return ErrUpdateEmptyString
	}

	hash, err := s.table.HashStringAndScreen(key)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			summary := newArrayOfNumbersSummary[V](s.numberOfValuesInSummary)
			summary.Update(values)

			s.table.Insert(index, entry[*ArrayOfNumbersSummary[V]]{Hash: hash, Summary: summary})
			return nil
		}
		return err
	}

	s.table.entries[index].Summary.Update(values)
	return nil
}

// UpdateBytes updates this sketch with given data
func (s *ArrayOfNumbersUpdateSketch[V]) UpdateBytes(data []byte, values []V) error {
	hash, err := s.table.HashBytesAndScreen(data)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			summary := newArrayOfNumbersSummary[V](s.numberOfValuesInSummary)
			summary.Update(values)

			s.table.Insert(index, entry[*ArrayOfNumbersSummary[V]]{Hash: hash, Summary: summary})
			return nil
		}
		return err
	}

	s.table.entries[index].Summary.Update(values)
	return nil
}

// Trim removes retained entries in excess of the nominal size k (if any)
func (s *ArrayOfNumbersUpdateSketch[V]) Trim() {
	s.table.Trim()
}

// Reset resets the sketch to the initial empty state
func (s *ArrayOfNumbersUpdateSketch[V]) Reset() {
	s.table.Reset()
}

// NumValuesInSummary returns the number of values in ArrayOfNumbersSummary.
func (s *ArrayOfNumbersUpdateSketch[V]) NumValuesInSummary() uint8 {
	return s.numberOfValuesInSummary
}
