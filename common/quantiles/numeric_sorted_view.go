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

import (
	"errors"

	"github.com/apache/datasketches-go/internal"
	quantilesutils "github.com/apache/datasketches-go/internal/quantiles"
)

var ErrEmpty = errors.New("operation is undefined for an empty sketch")

// Number is a type constraint that permits numeric types used by quantile sketches.
type Number interface {
	float32 | float64 | int64
}

// The NumericSortedView provides a sorted view of the data retained by a numeric quantiles-type sketch that
// would be cumbersome to get any other way.
// One could use the sketch's iterator to iterate over the contents of the sketch,
// but the result would not be sorted.
//
// The data from a NumericSortedView is an unbiased random sample of the input stream that can be used for other kinds of
// analysis not directly provided by the sketch.
type NumericSortedView[T Number] struct {
	quantiles  []T
	cumWeights []int64
	n          int64
}

// NewNumericSortedView constructs a new NumericSortedView.
func NewNumericSortedView[T Number](
	quantiles []T, cumWeights []int64, n int64, maxItem, minItem T,
) *NumericSortedView[T] {
	res := quantilesutils.IncludeMinMax(quantiles, cumWeights, maxItem, minItem)
	return &NumericSortedView[T]{
		quantiles:  res.Quantiles,
		cumWeights: res.CumWeights,
		n:          n,
	}
}

// CumulativeWeights returns a copy of the cumulative weights slice.
// Also known as the natural ranks, which are the Natural Numbers on the interval [1, N].
func (s *NumericSortedView[T]) CumulativeWeights() []int64 {
	copied := make([]int64, len(s.cumWeights))
	copy(copied, s.cumWeights)
	return copied
}

// N returns the total number of items presented to the sourcing sketch.
func (s *NumericSortedView[T]) N() int64 {
	return s.n
}

// NumRetained returns the number of quantiles retained by this sorted view.
// This may be slightly different from the function with the same name when called from the originating sketch.
func (s *NumericSortedView[T]) NumRetained() int {
	return len(s.quantiles)
}

// IsEmpty returns true if the view is empty.
func (s *NumericSortedView[T]) IsEmpty() bool {
	return s.n == 0
}

// Iterator creates and returns a new iterator.
func (s *NumericSortedView[T]) Iterator() *NumericSortedViewIterator[T] {
	return NewNumericSortedViewIterator[T](s.quantiles, s.cumWeights)
}

// Rank returns the normalized rank corresponding to the given a quantile.
// If the sketch is empty, it returns an error.
func (s *NumericSortedView[T]) Rank(quantile T, isInclusive bool) (float64, error) {
	if s.IsEmpty() {
		return 0, ErrEmpty
	}

	crit := internal.InequalityLT
	if isInclusive {
		crit = internal.InequalityLE
	}

	index, err := internal.FindWithInequality[T](s.quantiles, 0, len(s.quantiles)-1, quantile, crit, func(t1 T, t2 T) bool {
		return t1 < t2
	})
	if err != nil {
		return 0, err
	}
	if index == -1 {
		return 0, nil //LT case: quantile <= minQuantile; LE case: quantile < minQuantile
	}
	return float64(s.cumWeights[index]) / float64(s.n), nil
}

// Quantiles return a copy of the quantiles slice.
func (s *NumericSortedView[T]) Quantiles() []T {
	copied := make([]T, len(s.quantiles))
	copy(copied, s.quantiles)
	return copied
}

// Quantile returns the approximate quantile of the given normalized rank.
// If inclusive, the given rank includes all quantiles less than or equal to
// the quantile directly corresponding to the given rank.
// If not, the given rank includes all quantiles less than
// the quantile directly corresponding to the given rank.
// If the sketch is empty, it returns an error.
func (s *NumericSortedView[T]) Quantile(rank float64, isInclusive bool) (T, error) {
	if s.IsEmpty() {
		return 0, ErrEmpty
	}

	if err := quantilesutils.ValidateNormalizedRankBounds(rank); err != nil {
		return 0, err
	}

	naturalRank := quantilesutils.ComputeNaturalRank(rank, uint64(s.n), isInclusive)
	crit := internal.InequalityGT
	if isInclusive {
		crit = internal.InequalityGE
	}
	length := len(s.cumWeights)
	index, err := internal.FindWithInequality(
		s.cumWeights, 0, length-1, naturalRank, crit, func(a, b int64) bool { return a < b },
	)
	if err != nil {
		return 0, err
	}
	if index == -1 {
		return s.quantiles[length-1], nil
	}
	return s.quantiles[index], nil
}

// MinItem returns the minimum item in the sketch.
// If the sketch is empty, it returns an error.
// This may be distinct from the smallest item retained by the sketch algorithm.
func (s *NumericSortedView[T]) MinItem() (T, error) {
	if s.IsEmpty() {
		return 0, ErrEmpty
	}
	return s.quantiles[0], nil
}

// MaxItem returns the maximum item in the view.
// If the sketch is empty, it returns an error.
// This may be distinct from the largest item retained by the sketch algorithm.
func (s *NumericSortedView[T]) MaxItem() (T, error) {
	if s.IsEmpty() {
		return 0, ErrEmpty
	}
	return s.quantiles[len(s.quantiles)-1], nil
}

// CDF returns an approximation of the stream's cumulative distribution
// function for the given split points.
//
// It returns a monotonically increasing slice of cumulative probabilities in
// [0, 1]. The returned slice has len(splitPoints)+1 entries.
//
// The approximation has the probabilistic guarantee implied by
// NormalizedRankError(false).
//
// splitPoints must be unique and strictly increasing. They divide the item
// domain into len(splitPoints)+1 overlapping intervals. Each interval starts
// below the lowest retained item, which corresponds to cumulative probability
// 0, and ends at the cumulative probability of its split point.
//
// The final interval represents the rest of the distribution, so the last
// returned value is always 1.
//
// If a split point is exactly equal to a retained item, isInclusive=true includes
// that item's weight in the cumulative probability for that split point.
//
// Callers generally should not include the true minimum or maximum stream item
// in splitPoints.
func (s *NumericSortedView[T]) CDF(splitPoints []T, isInclusive bool) ([]float64, error) {
	if err := quantilesutils.ValidateSplitPoints(splitPoints); err != nil {
		return nil, err
	}

	length := len(splitPoints) + 1
	buckets := make([]float64, length)
	for i := 0; i < length-1; i++ {
		rank, err := s.Rank(splitPoints[i], isInclusive)
		if err != nil {
			return nil, err
		}

		buckets[i] = rank
	}
	buckets[length-1] = 1.0
	return buckets, nil
}

// PMF returns an approximation of the stream's probability mass function
// for the given split points.
//
// It returns len(splitPoints)+1 probability masses in [0, 1]. The returned
// intervals are consecutive and non-overlapping, and their sum is always 1.
//
// The approximation has the probabilistic guarantee implied by
// NormalizedRankError(true).
//
// splitPoints must be unique and strictly increasing. They divide the item
// domain into len(splitPoints)+1 intervals. Each interior interval starts at
// one split point and ends at the next. The first interval starts below the
// lowest retained item and ends at the first split point. The last interval
// starts at the last split point and extends past the largest retained item.
//
// If a split point is exactly equal to a retained item, the interval boundary
// handling depends on searchCrit. With isInclusive=true, an interval includes an item
// equal to its upper split point and excludes an item equal to its lower split
// point. With isInclusive=false, an interval excludes an item equal to its upper split
// point and includes an item equal to its lower split point.
//
// Callers generally should not include the true minimum or maximum stream item
// in splitPoints.
func (s *NumericSortedView[T]) PMF(splitPoints []T, isInclusive bool) ([]float64, error) {
	buckets, err := s.CDF(splitPoints, isInclusive)
	if err != nil {
		return nil, err
	}

	for i := len(buckets) - 1; i > 0; i-- {
		buckets[i] -= buckets[i-1]
	}
	return buckets, nil
}
