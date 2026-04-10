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

package quantiles

import "errors"

var ErrIndexOutOfValidRange = errors.New("index out of range")

// Number is a type constraint that permits numeric types used by quantile sketches.
type Number interface {
	float32 | float64 | int64
}

// NumericSortedViewIterator is an iterator over sorted views of numeric quantile sketches of type.
type NumericSortedViewIterator[C Number] struct {
	cumWeights []int64
	quantiles  []C
	totalN     int64
	index      int
}

// NewSortedViewIterator constructs a new NumericSortedViewIterator.
// The quantiles slice must be ordered and have the same length as cumWeights.
// The cumWeights slice must be ordered, start with the value one, and the last
// value must be equal to N, the total number of items updated to the sketch.
func NewSortedViewIterator[C Number](quantiles []C, cumWeights []int64) *NumericSortedViewIterator[C] {
	var totalN int64
	if len(cumWeights) > 0 {
		totalN = cumWeights[len(cumWeights)-1]
	}

	return &NumericSortedViewIterator[C]{
		cumWeights: cumWeights,
		totalN:     totalN,
		index:      -1,
		quantiles:  quantiles,
	}
}

func (it *NumericSortedViewIterator[C]) validateIndex() error {
	if it.index < 0 || it.index >= len(it.cumWeights) {
		return ErrIndexOutOfValidRange
	}
	return nil
}

// NaturalRank returns the natural rank at the current index.
// This is equivalent to NaturalRankWithCriterion(Inclusive).
// NOTE: Call Next() before calling this method.
func (it *NumericSortedViewIterator[C]) NaturalRank() (int64, error) {
	if err := it.validateIndex(); err != nil {
		return 0, err
	}
	return it.cumWeights[it.index], nil
}

// NaturalRankWithCriterion returns the natural rank at the current index (or previous index)
// based on the chosen search criterion. The natural rank is a number in the range [1, N],
// where N (N()) is the total number of items fed to the sketch.
//
// If inclusive, includes the weight of the item at the current index.
// Otherwise, returns the natural rank of the previous index.
// NOTE: Call Next() before calling this method.
func (it *NumericSortedViewIterator[C]) NaturalRankWithCriterion(isInclusive bool) (int64, error) {
	if err := it.validateIndex(); err != nil {
		return 0, err
	}
	if isInclusive {
		return it.cumWeights[it.index], nil
	}
	if it.index == 0 {
		return 0, nil
	}
	return it.cumWeights[it.index-1], nil
}

// N returns the total count of all items presented to the sketch.
func (it *NumericSortedViewIterator[C]) N() int64 {
	return it.totalN
}

// NormalizedRank returns the normalized rank at the current index.
// This is equivalent to NormalizedRankWithCriterion(true).
// NOTE: Call Next() before calling this method.
func (it *NumericSortedViewIterator[C]) NormalizedRank() (float64, error) {
	nr, err := it.NaturalRank()
	if err != nil {
		return 0, err
	}
	return float64(nr) / float64(it.totalN), nil
}

// NormalizedRankWithCriterion returns the normalized rank at the current index (or previous
// index) based on the chosen search criterion. Normalized rank = natural rank / N (N())
// and is a fraction in the range (0, 1.0].
// NOTE: Call Next() before calling this method.
func (it *NumericSortedViewIterator[C]) NormalizedRankWithCriterion(isInclusive bool) (float64, error) {
	nr, err := it.NaturalRankWithCriterion(isInclusive)
	if err != nil {
		return 0, err
	}
	return float64(nr) / float64(it.totalN), nil
}

// Weight returns the weight contribution of the item at the current index.
// NOTE: Call Next() before calling this method.
func (it *NumericSortedViewIterator[C]) Weight() (int64, error) {
	if err := it.validateIndex(); err != nil {
		return 0, err
	}
	if it.index == 0 {
		return it.cumWeights[0], nil
	}
	return it.cumWeights[it.index] - it.cumWeights[it.index-1], nil
}

// Next advances the index and checks if it is valid.
// The state of the iterator is undefined before the first call of this method.
func (it *NumericSortedViewIterator[C]) Next() bool {
	it.index++
	return it.index < len(it.cumWeights)
}

// Quantile returns the quantile at the current index.
// NOTE: Call Next() before calling this method.
func (it *NumericSortedViewIterator[C]) Quantile() (C, error) {
	if err := it.validateIndex(); err != nil {
		var zero C
		return zero, err
	}
	return it.quantiles[it.index], nil
}
