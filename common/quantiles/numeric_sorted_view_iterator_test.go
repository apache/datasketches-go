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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortedViewIteratorNext(t *testing.T) {
	tests := []struct {
		name       string
		quantiles  []int
		cumWeights []int64
		wantCount  int
	}{
		{"empty", []int{}, []int64{}, 0},
		{"single element", []int{42}, []int64{1}, 1},
		{"multiple elements", []int{10, 20, 30}, []int64{1, 3, 6}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewSortedViewIterator(tt.quantiles, tt.cumWeights)
			count := 0
			for it.Next() {
				count++
			}
			assert.Equal(t, tt.wantCount, count)
			assert.False(t, it.Next())
		})
	}
}

func TestSortedViewIteratorQuantile(t *testing.T) {
	tests := []struct {
		name      string
		quantiles []int
	}{
		{"single element", []int{42}},
		{"multiple elements", []int{10, 20, 30}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cumWeights := make([]int64, len(tt.quantiles))
			for i := range cumWeights {
				cumWeights[i] = int64(i + 1)
			}
			it := NewSortedViewIterator(tt.quantiles, cumWeights)
			var got []int
			for it.Next() {
				v, err := it.Quantile()
				require.NoError(t, err)
				got = append(got, v)
			}
			assert.Equal(t, tt.quantiles, got)
		})
	}
}

func TestSortedViewIteratorN(t *testing.T) {
	tests := []struct {
		name       string
		cumWeights []int64
		wantN      int64
	}{
		{"empty", []int64{}, 0},
		{"single element", []int64{5}, 5},
		{"multiple elements", []int64{1, 3, 6}, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewSortedViewIterator([]int{}, tt.cumWeights)
			assert.Equal(t, tt.wantN, it.N())
		})
	}
}

func TestSortedViewIteratorNaturalRank(t *testing.T) {
	tests := []struct {
		name       string
		cumWeights []int64
	}{
		{"single element", []int64{1}},
		{"uniform weights", []int64{1, 2, 3, 4}},
		{"varying weights", []int64{2, 5, 7, 10}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quantiles := make([]int, len(tt.cumWeights))
			it := NewSortedViewIterator(quantiles, tt.cumWeights)
			i := 0
			for it.Next() {
				got, err := it.NaturalRank()
				require.NoError(t, err)
				assert.Equal(t, tt.cumWeights[i], got)
				i++
			}
		})
	}
}

func TestSortedViewIteratorNaturalRankWithCriterion(t *testing.T) {
	cumWeights := []int64{2, 5, 7, 10}
	quantiles := make([]int, len(cumWeights))

	tests := []struct {
		name        string
		isInclusive bool
		wantRanks   []int64
	}{
		{"inclusive", true, []int64{2, 5, 7, 10}},
		{"exclusive", false, []int64{0, 2, 5, 7}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewSortedViewIterator(quantiles, cumWeights)
			i := 0
			for it.Next() {
				got, err := it.NaturalRankWithCriterion(tt.isInclusive)
				require.NoError(t, err)
				assert.Equal(t, tt.wantRanks[i], got)
				i++
			}
		})
	}
}

func TestSortedViewIteratorNormalizedRank(t *testing.T) {
	tests := []struct {
		name       string
		cumWeights []int64
		wantRanks  []float64
	}{
		{"single element", []int64{1}, []float64{1.0}},
		{"uniform weights", []int64{1, 2, 3}, []float64{1.0 / 3, 2.0 / 3, 1.0}},
		{"varying weights", []int64{2, 5, 10}, []float64{0.2, 0.5, 1.0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quantiles := make([]int, len(tt.cumWeights))
			it := NewSortedViewIterator(quantiles, tt.cumWeights)
			i := 0
			for it.Next() {
				got, err := it.NormalizedRank()
				require.NoError(t, err)
				assert.InDelta(t, tt.wantRanks[i], got, 1e-12)
				i++
			}
		})
	}
}

func TestSortedViewIteratorNormalizedRankWithCriterion(t *testing.T) {
	cumWeights := []int64{2, 5, 10}
	quantiles := make([]int, len(cumWeights))
	totalN := float64(10)

	tests := []struct {
		name        string
		isInclusive bool
		wantRanks   []float64
	}{
		{"inclusive", true, []float64{2 / totalN, 5 / totalN, 10 / totalN}},
		{"exclusive", false, []float64{0 / totalN, 2 / totalN, 5 / totalN}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := NewSortedViewIterator(quantiles, cumWeights)
			i := 0
			for it.Next() {
				got, err := it.NormalizedRankWithCriterion(tt.isInclusive)
				require.NoError(t, err)
				assert.InDelta(t, tt.wantRanks[i], got, 1e-12)
				i++
			}
		})
	}
}

func TestSortedViewIteratorWeight(t *testing.T) {
	tests := []struct {
		name        string
		cumWeights  []int64
		wantWeights []int64
	}{
		{"single element", []int64{3}, []int64{3}},
		{"uniform weights", []int64{1, 2, 3, 4}, []int64{1, 1, 1, 1}},
		{"varying weights", []int64{2, 5, 7, 10}, []int64{2, 3, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quantiles := make([]int, len(tt.cumWeights))
			it := NewSortedViewIterator(quantiles, tt.cumWeights)
			i := 0
			for it.Next() {
				got, err := it.Weight()
				require.NoError(t, err)
				assert.Equal(t, tt.wantWeights[i], got)
				i++
			}
		})
	}
}

func TestSortedViewIteratorInvalidIndex(t *testing.T) {
	quantiles := []int{10, 20, 30}
	cumWeights := []int64{1, 3, 6}

	tests := []struct {
		name  string
		setup func() *NumericSortedViewIterator[int]
	}{
		{
			"before first Next",
			func() *NumericSortedViewIterator[int] {
				return NewSortedViewIterator(quantiles, cumWeights)
			},
		},
		{
			"after iteration exhausted",
			func() *NumericSortedViewIterator[int] {
				it := NewSortedViewIterator(quantiles, cumWeights)
				for it.Next() {
				}
				return it
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := tt.setup()

			_, err := it.Quantile()
			assert.ErrorIs(t, err, ErrIndexOutOfValidRange)

			_, err = it.NaturalRank()
			assert.ErrorIs(t, err, ErrIndexOutOfValidRange)

			_, err = it.NaturalRankWithCriterion(true)
			assert.ErrorIs(t, err, ErrIndexOutOfValidRange)

			_, err = it.NormalizedRank()
			assert.ErrorIs(t, err, ErrIndexOutOfValidRange)

			_, err = it.NormalizedRankWithCriterion(true)
			assert.ErrorIs(t, err, ErrIndexOutOfValidRange)

			_, err = it.Weight()
			assert.ErrorIs(t, err, ErrIndexOutOfValidRange)
		})
	}
}
