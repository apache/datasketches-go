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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type numericSortedViewFixture struct {
	name      string
	quantiles []int64
	weights   []int64
}

type numericSortedViewCriterion struct {
	name        string
	isInclusive bool
}

func numericSortedViewFixtures() []numericSortedViewFixture {
	return []numericSortedViewFixture{
		{
			name:      "single value",
			quantiles: []int64{10},
			weights:   []int64{1},
		},
		{
			name:      "two identical values",
			quantiles: []int64{10, 10},
			weights:   []int64{1, 1},
		},
		{
			name:      "uniform weights",
			quantiles: []int64{10, 20, 30, 40},
			weights:   []int64{2, 2, 2, 2},
		},
		{
			name:      "duplicates with equal weights",
			quantiles: []int64{10, 20, 20, 30, 30, 30, 40, 50},
			weights:   []int64{2, 2, 2, 2, 2, 2, 2, 2},
		},
		{
			name:      "duplicates with varying weights",
			quantiles: []int64{10, 10, 20, 20, 30, 30, 40, 40},
			weights:   []int64{2, 1, 2, 1, 2, 1, 2, 1},
		},
	}
}

func numericSortedViewCriteria() []numericSortedViewCriterion {
	return []numericSortedViewCriterion{
		{name: "Inclusive", isInclusive: true},
		{name: "Exclusive", isInclusive: false},
	}
}

func TestNewNumericSortedView(t *testing.T) {
	t.Run("Accessors", func(t *testing.T) {
		t.Run("Reinserts Missing Min And Max", func(t *testing.T) {
			view := NewNumericSortedView([]int64{20, 40}, []int64{2, 4}, 4, 50, 10)

			minItem, err := view.MinItem()
			require.NoError(t, err)
			maxItem, err := view.MaxItem()
			require.NoError(t, err)

			assert.Equal(t, []int64{10, 20, 40, 50}, view.Quantiles())
			assert.Equal(t, []int64{1, 2, 3, 4}, view.CumulativeWeights())
			assert.Equal(t, int64(10), minItem)
			assert.Equal(t, int64(50), maxItem)
			assert.Equal(t, 4, view.NumRetained())
		})
	})
}

func TestNumericSortedViewCumulativeWeights(t *testing.T) {
	t.Run("Returns Copy", func(t *testing.T) {
		view := NewNumericSortedView([]int64{10, 20, 30}, []int64{1, 2, 3}, 3, 30, 10)

		got := view.CumulativeWeights()
		got[0] = 999

		assert.Equal(t, []int64{1, 2, 3}, view.CumulativeWeights())
	})
}

func TestNumericSortedViewNumRetained(t *testing.T) {
	t.Run("Returns Retained Count", func(t *testing.T) {
		for _, fixture := range numericSortedViewFixtures() {
			fixture := fixture
			t.Run(fixture.name, func(t *testing.T) {
				view, _ := buildInt64NumericSortedView(fixture.quantiles, fixture.weights)
				assert.Equal(t, len(fixture.quantiles), view.NumRetained())
			})
		}
	})

	t.Run("Includes Reinserted Endpoints", func(t *testing.T) {
		view := NewNumericSortedView([]int64{20, 40}, []int64{2, 4}, 4, 50, 10)
		assert.Equal(t, 4, view.NumRetained())
	})
}

func TestNumericSortedViewIsEmpty(t *testing.T) {
	t.Run("False For NonEmpty View", func(t *testing.T) {
		view := NewNumericSortedView([]int64{10, 20, 30}, []int64{1, 2, 3}, 3, 30, 10)
		assert.False(t, view.IsEmpty())
	})

	t.Run("True When N Is Zero", func(t *testing.T) {
		assert.True(t, newEmptyNumericSortedView().IsEmpty())
	})
}

func TestNumericSortedViewIterator(t *testing.T) {
	t.Run("Traverses Adjusted View", func(t *testing.T) {
		view := NewNumericSortedView([]int64{20, 40}, []int64{2, 4}, 4, 50, 10)
		it := view.Iterator()

		var gotQuantiles []int64
		var gotNaturalRanks []int64
		for it.Next() {
			quantile, err := it.Quantile()
			require.NoError(t, err)
			naturalRank, err := it.NaturalRank()
			require.NoError(t, err)
			gotQuantiles = append(gotQuantiles, quantile)
			gotNaturalRanks = append(gotNaturalRanks, naturalRank)
		}

		assert.Equal(t, []int64{10, 20, 40, 50}, gotQuantiles)
		assert.Equal(t, []int64{1, 2, 3, 4}, gotNaturalRanks)
	})
}

func TestNumericSortedViewRank(t *testing.T) {
	for _, fixture := range numericSortedViewFixtures() {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			view, stream := buildInt64NumericSortedView(fixture.quantiles, fixture.weights)
			maxQuery := stream[len(stream)-1] + 5

			for _, criterion := range numericSortedViewCriteria() {
				criterion := criterion
				t.Run(criterion.name, func(t *testing.T) {
					for q := int64(5); q <= maxQuery; q += 5 {
						got, err := view.Rank(q, criterion.isInclusive)
						require.NoError(t, err)
						want := expectedRankFromStream(stream, q, criterion.isInclusive)
						assert.InDelta(t, want, got, 1e-12, "quantile=%d", q)
					}
				})
			}
		})
	}

	t.Run("Returns ErrEmpty For Empty View", func(t *testing.T) {
		_, err := newEmptyNumericSortedView().Rank(1, true)
		assert.ErrorIs(t, err, ErrEmpty)
	})
}

func TestNumericSortedViewQuantiles(t *testing.T) {
	t.Run("Returns Copy", func(t *testing.T) {
		view := NewNumericSortedView([]int64{10, 20, 30}, []int64{1, 2, 3}, 3, 30, 10)

		got := view.Quantiles()
		got[0] = 999

		assert.Equal(t, []int64{10, 20, 30}, view.Quantiles())
	})

	t.Run("Returns Adjusted Quantiles", func(t *testing.T) {
		view := NewNumericSortedView([]int64{20, 40}, []int64{2, 4}, 4, 50, 10)
		assert.Equal(t, []int64{10, 20, 40, 50}, view.Quantiles())
	})
}

func TestNumericSortedViewQuantile(t *testing.T) {
	for _, fixture := range numericSortedViewFixtures() {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			view, stream := buildInt64NumericSortedView(fixture.quantiles, fixture.weights)
			doubleN := len(stream) * 2

			for _, criterion := range numericSortedViewCriteria() {
				criterion := criterion
				t.Run(criterion.name, func(t *testing.T) {
					for i := 0; i <= doubleN; i++ {
						rank := float64(i) / float64(doubleN)
						got, err := view.Quantile(rank, criterion.isInclusive)
						require.NoError(t, err)
						want := expectedQuantileFromStream(stream, rank, criterion.isInclusive)
						assert.Equal(t, want, got, "rank=%f", rank)
					}
				})
			}
		})
	}

	t.Run("Rejects Invalid Rank Bounds", func(t *testing.T) {
		view := NewNumericSortedView([]int64{10, 20, 30}, []int64{1, 2, 3}, 3, 30, 10)

		_, err := view.Quantile(-0.1, true)
		assert.EqualError(t, err, "rank must be between 0 and 1 inclusive")

		_, err = view.Quantile(1.1, false)
		assert.EqualError(t, err, "rank must be between 0 and 1 inclusive")
	})

	t.Run("Returns ErrEmpty For Empty View", func(t *testing.T) {
		_, err := newEmptyNumericSortedView().Quantile(0.5, false)
		assert.ErrorIs(t, err, ErrEmpty)
	})
}

func TestNumericSortedViewMinItem(t *testing.T) {
	t.Run("Returns Minimum Item", func(t *testing.T) {
		for _, fixture := range numericSortedViewFixtures() {
			fixture := fixture
			t.Run(fixture.name, func(t *testing.T) {
				view, stream := buildInt64NumericSortedView(fixture.quantiles, fixture.weights)
				got, err := view.MinItem()
				require.NoError(t, err)
				assert.Equal(t, stream[0], got)
			})
		}
	})

	t.Run("Returns Reinserted Minimum", func(t *testing.T) {
		view := NewNumericSortedView([]int64{20, 40}, []int64{2, 4}, 4, 50, 10)
		got, err := view.MinItem()
		require.NoError(t, err)
		assert.Equal(t, int64(10), got)
	})

	t.Run("Returns ErrEmpty For Empty View", func(t *testing.T) {
		_, err := newEmptyNumericSortedView().MinItem()
		assert.ErrorIs(t, err, ErrEmpty)
	})
}

func TestNumericSortedViewMaxItem(t *testing.T) {
	t.Run("Returns Maximum Item", func(t *testing.T) {
		for _, fixture := range numericSortedViewFixtures() {
			fixture := fixture
			t.Run(fixture.name, func(t *testing.T) {
				view, stream := buildInt64NumericSortedView(fixture.quantiles, fixture.weights)
				got, err := view.MaxItem()
				require.NoError(t, err)
				assert.Equal(t, stream[len(stream)-1], got)
			})
		}
	})

	t.Run("Returns Reinserted Maximum", func(t *testing.T) {
		view := NewNumericSortedView([]int64{20, 40}, []int64{2, 4}, 4, 50, 10)
		got, err := view.MaxItem()
		require.NoError(t, err)
		assert.Equal(t, int64(50), got)
	})

	t.Run("Returns ErrEmpty For Empty View", func(t *testing.T) {
		_, err := newEmptyNumericSortedView().MaxItem()
		assert.ErrorIs(t, err, ErrEmpty)
	})
}

func TestNumericSortedViewCDF(t *testing.T) {
	t.Run("Matches Rank Queries", func(t *testing.T) {
		view, stream := buildInt64NumericSortedView(
			[]int64{10, 20, 20, 30, 30, 30, 40, 50},
			[]int64{2, 2, 2, 2, 2, 2, 2, 2},
		)
		splitPoints := []int64{20, 30, 40}

		for _, criterion := range numericSortedViewCriteria() {
			criterion := criterion
			t.Run(criterion.name, func(t *testing.T) {
				got, err := view.CDF(splitPoints, criterion.isInclusive)
				require.NoError(t, err)

				want := make([]float64, len(splitPoints)+1)
				for i, splitPoint := range splitPoints {
					want[i] = expectedRankFromStream(stream, splitPoint, criterion.isInclusive)
				}
				want[len(want)-1] = 1.0

				assert.Len(t, got, len(want))
				for i := range want {
					assert.InDelta(t, want[i], got[i], 1e-12)
				}
			})
		}
	})

	t.Run("Rejects Invalid Split Points", func(t *testing.T) {
		view := NewNumericSortedView([]int64{10, 20, 30}, []int64{1, 2, 3}, 3, 30, 10)

		_, err := view.CDF([]int64{30, 20}, true)
		assert.Error(t, err)

		_, err = view.CDF([]int64{20, 20}, false)
		assert.Error(t, err)
	})

	t.Run("Returns ErrEmpty For Empty View", func(t *testing.T) {
		_, err := newEmptyNumericSortedView().CDF([]int64{1}, true)
		assert.ErrorIs(t, err, ErrEmpty)
	})
}

func TestNumericSortedViewPMF(t *testing.T) {
	t.Run("Matches Interval Masses", func(t *testing.T) {
		view, stream := buildInt64NumericSortedView(
			[]int64{10, 20, 20, 30, 30, 30, 40, 50},
			[]int64{2, 2, 2, 2, 2, 2, 2, 2},
		)
		splitPoints := []int64{20, 30, 40}

		for _, criterion := range numericSortedViewCriteria() {
			criterion := criterion
			t.Run(criterion.name, func(t *testing.T) {
				got, err := view.PMF(splitPoints, criterion.isInclusive)
				require.NoError(t, err)

				wantCDF := make([]float64, len(splitPoints)+1)
				for i, splitPoint := range splitPoints {
					wantCDF[i] = expectedRankFromStream(stream, splitPoint, criterion.isInclusive)
				}
				wantCDF[len(wantCDF)-1] = 1.0

				wantPMF := append([]float64(nil), wantCDF...)
				for i := len(wantPMF) - 1; i > 0; i-- {
					wantPMF[i] -= wantPMF[i-1]
				}

				assert.Len(t, got, len(wantPMF))
				for i := range wantPMF {
					assert.InDelta(t, wantPMF[i], got[i], 1e-12)
				}
			})
		}
	})

	t.Run("Rejects Invalid Split Points", func(t *testing.T) {
		view := NewNumericSortedView([]int64{10, 20, 30}, []int64{1, 2, 3}, 3, 30, 10)

		_, err := view.PMF([]int64{30, 20}, true)
		assert.Error(t, err)

		_, err = view.PMF([]int64{20, 20}, false)
		assert.Error(t, err)
	})

	t.Run("Returns ErrEmpty For Empty View", func(t *testing.T) {
		_, err := newEmptyNumericSortedView().PMF([]int64{1}, false)
		assert.ErrorIs(t, err, ErrEmpty)
	})
}

func newEmptyNumericSortedView() *NumericSortedView[int64] {
	return NewNumericSortedView([]int64{1}, []int64{1}, 0, 1, 1)
}

func buildInt64NumericSortedView(quantiles []int64, weights []int64) (*NumericSortedView[int64], []int64) {
	cumWeights := cumulativeWeights(weights)
	view := NewNumericSortedView(
		append([]int64(nil), quantiles...),
		cumWeights,
		cumWeights[len(cumWeights)-1],
		quantiles[len(quantiles)-1],
		quantiles[0],
	)
	return view, expandWeightedValues(quantiles, weights)
}

func cumulativeWeights(weights []int64) []int64 {
	out := make([]int64, len(weights))
	var total int64
	for i, weight := range weights {
		total += weight
		out[i] = total
	}
	return out
}

func expandWeightedValues(quantiles []int64, weights []int64) []int64 {
	total := cumulativeWeights(weights)
	out := make([]int64, 0, total[len(total)-1])
	for i, quantile := range quantiles {
		for w := int64(0); w < weights[i]; w++ {
			out = append(out, quantile)
		}
	}
	return out
}

func expectedRankFromStream(stream []int64, quantile int64, isInclusive bool) float64 {
	var count int
	for _, value := range stream {
		if isInclusive {
			if value <= quantile {
				count++
			}
			continue
		}
		if value < quantile {
			count++
		}
	}
	return float64(count) / float64(len(stream))
}

func expectedQuantileFromStream(stream []int64, rank float64, isInclusive bool) int64 {
	n := len(stream)
	if isInclusive {
		index := int(math.Ceil(rank*float64(n))) - 1
		if index < 0 {
			index = 0
		}
		return stream[index]
	}

	index := int(math.Floor(rank * float64(n)))
	if index >= n {
		index = n - 1
	}
	return stream[index]
}
