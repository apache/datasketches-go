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

package req

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/internal/quantiles"
)

func loadSketch(t *testing.T, k, min, max int, up, hra bool) *Sketch {
	t.Helper()
	sk, err := NewSketch(WithK(k), WithHighRankAccuracyMode(hra))
	assert.NoError(t, err)
	if up {
		for i := min; i <= max; i++ {
			assert.NoError(t, sk.Update(float32(i)))
		}
	} else {
		for i := max; i >= min; i-- {
			assert.NoError(t, sk.Update(float32(i)))
		}
	}
	return sk
}

func evenlySpacedFloats(min, max float32, n int) []float32 {
	if n < 2 {
		return []float32{min}
	}
	result := make([]float32, n)
	step := (max - min) / float32(n-1)
	for i := 0; i < n; i++ {
		result[i] = min + float32(i)*step
	}
	result[n-1] = max // avoid floating point drift
	return result
}

func evenlySpacedDoubles(min, max float64, n int) []float64 {
	if n < 2 {
		return []float64{min}
	}
	result := make([]float64, n)
	step := (max - min) / float64(n-1)
	for i := 0; i < n; i++ {
		result[i] = min + float64(i)*step
	}
	result[n-1] = max
	return result
}

func TestNewSketch(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		assert.Equal(t, 12, sk.K())
	})

	t.Run("with options", func(t *testing.T) {
		sk, err := NewSketch(WithK(50), WithHighRankAccuracyMode(true))
		assert.NoError(t, err)
		assert.Equal(t, 50, sk.K())
		assert.Equal(t, true, sk.IsHighRankAccuracyMode())
	})

	t.Run("invalid k", func(t *testing.T) {
		_, err := NewSketch(WithK(1))
		assert.ErrorContains(t, err, "must be even and in the range [4, 1024]")
	})
}

func TestSketchCDF(t *testing.T) {
	t.Run("NaN", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(1))
		_, err = sk.CDF([]float32{float32(math.NaN())}, true)
		assert.ErrorIs(t, err, quantiles.ErrNanInSplitPoints)
	})

	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)
		spArr := []float32{20, 40, 60, 80}
		cdf, err := sk.CDF(spArr, true)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr)+1, len(cdf))
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)
		spArr := []float32{20, 40, 60, 80}
		cdf, err := sk.CDF(spArr, true)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr)+1, len(cdf))
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)
		spArr := []float32{20, 40, 60, 80}
		cdf, err := sk.CDF(spArr, true)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr)+1, len(cdf))
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)
		spArr := []float32{20, 40, 60, 80}
		cdf, err := sk.CDF(spArr, true)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr)+1, len(cdf))
	})
}

func TestSketchQuantile(t *testing.T) {
	t.Run("Exceed limit", func(t *testing.T) {
		sk := loadSketch(t, 6, 1, 200, true, true)
		_, err := sk.Quantile(2.0)
		assert.ErrorContains(t, err, "rank must be between 0 and 1 inclusive")
		_, err = sk.Quantile(-2.0)
		assert.ErrorContains(t, err, "rank must be between 0 and 1 inclusive")
	})

	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)
		rArr := []float64{0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0}
		qOut, err := sk.Quantiles(rArr)
		assert.NoError(t, err)
		assert.Equal(t, len(rArr), len(qOut))
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)
		rArr := []float64{0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0}
		qOut, err := sk.Quantiles(rArr)
		assert.NoError(t, err)
		assert.Equal(t, len(rArr), len(qOut))
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)
		rArr := []float64{0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0}
		qOut, err := sk.Quantiles(rArr)
		assert.NoError(t, err)
		assert.Equal(t, len(rArr), len(qOut))
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)
		rArr := []float64{0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0}
		qOut, err := sk.Quantiles(rArr)
		assert.NoError(t, err)
		assert.Equal(t, len(rArr), len(qOut))
	})

	t.Run("exclusive", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		for i := 1; i <= 10; i++ {
			assert.NoError(t, sk.Update(float32(i)))
		}

		expectedExcl := []struct {
			rank float64
			q    float32
		}{
			{0, 1}, {0.1, 2}, {0.2, 3}, {0.3, 4}, {0.4, 5},
			{0.5, 6}, {0.6, 7}, {0.7, 8}, {0.8, 9}, {0.9, 10}, {1, 10},
		}
		for _, tc := range expectedExcl {
			q, err := sk.Quantile(tc.rank, WithExclusiveSearch())
			assert.NoError(t, err)
			assert.Equal(t, tc.q, q, "exclusive quantile mismatch at rank=%f", tc.rank)
		}
	})

	t.Run("inclusive", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		for i := 1; i <= 10; i++ {
			assert.NoError(t, sk.Update(float32(i)))
		}

		expectedIncl := []struct {
			rank float64
			q    float32
		}{
			{0, 1}, {0.1, 1}, {0.2, 2}, {0.3, 3}, {0.4, 4},
			{0.5, 5}, {0.6, 6}, {0.7, 7}, {0.8, 8}, {0.9, 9}, {1, 10},
		}
		for _, tc := range expectedIncl {
			q, err := sk.Quantile(tc.rank)
			assert.NoError(t, err)
			assert.Equal(t, tc.q, q, "inclusive quantile mismatch at rank=%f", tc.rank)
		}
	})

	t.Run("Quantile and Quantiles equivalence", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		for i := 1; i <= 10; i++ {
			assert.NoError(t, sk.Update(float32(i)))
		}

		ranks := []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}

		// exclusive
		quantilesExcl, err := sk.Quantiles(ranks, WithExclusiveSearch())
		assert.NoError(t, err)
		for i, r := range ranks {
			q, err := sk.Quantile(r, WithExclusiveSearch())
			assert.NoError(t, err)
			assert.Equal(t, q, quantilesExcl[i])
		}

		// inclusive
		quantilesIncl, err := sk.Quantiles(ranks)
		assert.NoError(t, err)
		for i, r := range ranks {
			q, err := sk.Quantile(r)
			assert.NoError(t, err)
			assert.Equal(t, q, quantilesIncl[i])
		}
	})
}

func TestSketchIsEstimationMode(t *testing.T) {
	sk := loadSketch(t, 20, 1, 119, true, true)
	assert.False(t, sk.IsEstimationMode())

	lb, err := sk.RankLowerBound(1.0, WithNumStdDev(1))
	assert.NoError(t, err)
	assert.Equal(t, 1.0, lb)

	ub, err := sk.RankUpperBound(1.0, WithNumStdDev(1))
	assert.NoError(t, err)
	assert.Equal(t, 1.0, ub)

	assert.Equal(t, 120, sk.maxNomSize)

	assert.NoError(t, sk.Update(120))
	assert.True(t, sk.IsEstimationMode())

	assert.Equal(t, 240, sk.maxNomSize)

	v, err := sk.Quantile(1.0)
	assert.NoError(t, err)
	assert.Equal(t, float32(120.0), v)

	sv, err := sk.SortedView()
	assert.NoError(t, err)
	assert.NotNil(t, sv)

	assert.True(t, ComputeRSE(sk.K(), 0.5, false, 120) >= 0)
}

func TestSketchUpdate(t *testing.T) {
	t.Run("NaN", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(float32(math.NaN())))
		assert.True(t, sk.IsEmpty())
	})
}

func TestSketchRank(t *testing.T) {
	t.Run("NaN", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(1))
		_, err = sk.Rank(float32(math.NaN()))
		assert.ErrorContains(t, err, "quantile must not be NaN")
	})

	t.Run("Infinity", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)

		err = sk.Update(1)
		assert.NoError(t, err)

		_, err = sk.Rank(float32(math.Inf(0)))
		assert.ErrorContains(t, err, "quantile must be finite")

		_, err = sk.Rank(float32(math.Inf(-1)))
		assert.ErrorContains(t, err, "quantile must be finite")
	})

	t.Run("Duplicated values", func(t *testing.T) {
		sk, err := NewSketch(WithK(50), WithHighRankAccuracyMode(false))
		assert.NoError(t, err)

		vArr := []float32{5, 5, 5, 6, 6, 6, 7, 8, 8, 8}
		for _, v := range vArr {
			assert.NoError(t, sk.Update(v))
		}

		// exclusive ranks
		rArrExcl := []float64{0.0, 0.0, 0.0, 0.3, 0.3, 0.3, 0.6, 0.7, 0.7, 0.7}
		for i, v := range vArr {
			rank, err := sk.Rank(v, WithExclusiveSearch())
			assert.NoError(t, err)
			assert.Equal(t, rArrExcl[i], rank, "exclusive rank mismatch at index %d", i)
		}

		// inclusive ranks
		rArrIncl := []float64{0.3, 0.3, 0.3, 0.6, 0.6, 0.6, 0.7, 1.0, 1.0, 1.0}
		for i, v := range vArr {
			rank, err := sk.Rank(v)
			assert.NoError(t, err)
			assert.Equal(t, rArrIncl[i], rank, "inclusive rank mismatch at index %d", i)
		}
	})

	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)

		spArr := evenlySpacedFloats(0, float32(100), 11)
		trueRanks := evenlySpacedDoubles(0, 1.0, 11)
		for i := 0; i < len(spArr); i++ {
			rank, err := sk.Rank(spArr[i])
			assert.NoError(t, err)
			assert.InDelta(t, trueRanks[i], rank, 0.01, "rank mismatch at splitPoint %f", spArr[i])
		}

		ranks, err := sk.Ranks(spArr)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr), len(ranks))
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)

		spArr := evenlySpacedFloats(0, float32(100), 11)
		trueRanks := evenlySpacedDoubles(0, 1.0, 11)
		for i := 0; i < len(spArr); i++ {
			rank, err := sk.Rank(spArr[i])
			assert.NoError(t, err)
			assert.InDelta(t, trueRanks[i], rank, 0.01, "rank mismatch at splitPoint %f", spArr[i])
		}

		ranks, err := sk.Ranks(spArr)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr), len(ranks))
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)

		spArr := evenlySpacedFloats(0, float32(100), 11)
		trueRanks := evenlySpacedDoubles(0, 1.0, 11)
		for i := 0; i < len(spArr); i++ {
			rank, err := sk.Rank(spArr[i])
			assert.NoError(t, err)
			assert.InDelta(t, trueRanks[i], rank, 0.01, "rank mismatch at splitPoint %f", spArr[i])
		}

		ranks, err := sk.Ranks(spArr)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr), len(ranks))
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)

		spArr := evenlySpacedFloats(0, float32(100), 11)
		trueRanks := evenlySpacedDoubles(0, 1.0, 11)
		for i := 0; i < len(spArr); i++ {
			rank, err := sk.Rank(spArr[i])
			assert.NoError(t, err)
			assert.InDelta(t, trueRanks[i], rank, 0.01, "rank mismatch at splitPoint %f", spArr[i])
		}

		ranks, err := sk.Ranks(spArr)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr), len(ranks))
	})

	t.Run("inclusive and exclusive", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		for i := 1; i <= 10; i++ {
			assert.NoError(t, sk.Update(float32(i)))
		}
		assert.False(t, sk.IsEmpty())
		assert.Equal(t, int64(10), sk.N())
		assert.Equal(t, 10, sk.NumRetained())

		for i := 1; i <= 10; i++ {
			rank, err := sk.Rank(float32(i))
			assert.NoError(t, err)
			assert.Equal(t, float64(i)/10.0, rank, "inclusive rank mismatch at i=%d", i)

			rankExcl, err := sk.Rank(float32(i), WithExclusiveSearch())
			assert.NoError(t, err)
			assert.Equal(t, float64(i-1)/10.0, rankExcl, "exclusive rank mismatch at i=%d", i)
		}
	})
}

func TestSketchMerge(t *testing.T) {
	t.Run("HRA mismatch", func(t *testing.T) {
		sk1, err := NewSketch(WithHighRankAccuracyMode(true))
		assert.NoError(t, err)
		assert.NoError(t, sk1.Update(1))

		sk2, err := NewSketch(WithHighRankAccuracyMode(false))
		assert.NoError(t, err)
		assert.NoError(t, sk2.Update(2))

		err = sk1.Merge(sk2)
		assert.ErrorContains(t, err, "both sketches must have the same HighRankAccuracy setting")
	})

	t.Run("Merge nil", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(1))
		assert.NoError(t, sk.Merge(nil))
		assert.Equal(t, int64(1), sk.N())
	})

	t.Run("Merge Multiple", func(t *testing.T) {
		s1, err := NewSketch(WithK(12))
		assert.NoError(t, err)
		for i := 0; i < 40; i++ {
			assert.NoError(t, s1.Update(float32(i)))
		}

		s2, err := NewSketch(WithK(12))
		assert.NoError(t, err)
		for i := 0; i < 40; i++ {
			assert.NoError(t, s2.Update(float32(i)))
		}

		s3, err := NewSketch(WithK(12))
		assert.NoError(t, err)
		for i := 0; i < 40; i++ {
			assert.NoError(t, s3.Update(float32(i)))
		}

		s, err := NewSketch(WithK(12))
		assert.NoError(t, err)
		assert.NoError(t, s.Merge(s1))
		assert.NoError(t, s.Merge(s2))
		assert.NoError(t, s.Merge(s3))
	})

	t.Run("Merge Empty", func(t *testing.T) {
		sk1, err := NewSketch()
		assert.NoError(t, err)
		sk2, err := NewSketch()
		assert.NoError(t, err)

		for i := 5; i < 10; i++ {
			assert.NoError(t, sk1.Update(float32(i)))
		}
		assert.NoError(t, sk1.Merge(sk2))
	})

	t.Run("Merge Overlapped", func(t *testing.T) {
		sk1, err := NewSketch()
		assert.NoError(t, err)
		sk2, err := NewSketch()
		assert.NoError(t, err)

		for i := 5; i < 10; i++ {
			assert.NoError(t, sk1.Update(float32(i)))
		}

		for i := 1; i <= 15; i++ {
			assert.NoError(t, sk2.Update(float32(i)))
		}
		assert.NoError(t, sk1.Merge(sk2))
		assert.Equal(t, int64(20), sk1.N())
	})

	t.Run("Merge Non-Overlapped", func(t *testing.T) {
		sk1, err := NewSketch()
		assert.NoError(t, err)
		sk2, err := NewSketch()
		assert.NoError(t, err)

		for i := 5; i < 10; i++ {
			assert.NoError(t, sk1.Update(float32(i)))
		}

		for i := 16; i <= 300; i++ {
			assert.NoError(t, sk2.Update(float32(i)))
		}
		assert.NoError(t, sk1.Merge(sk2))
	})

	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)
		sk2 := loadSketch(t, 20, 1, 100, true, true)
		assert.NoError(t, sk.Merge(sk2))
		assert.Equal(t, int64(200), sk.N())
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)
		sk2 := loadSketch(t, 20, 1, 100, false, false)
		assert.NoError(t, sk.Merge(sk2))
		assert.Equal(t, int64(200), sk.N())
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)
		sk2 := loadSketch(t, 20, 1, 100, false, true)
		assert.NoError(t, sk.Merge(sk2))
		assert.Equal(t, int64(200), sk.N())
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)
		sk2 := loadSketch(t, 20, 1, 100, true, false)
		assert.NoError(t, sk.Merge(sk2))
		assert.Equal(t, int64(200), sk.N())
	})
}

func TestSketchRankUBLB(t *testing.T) {
	tests := []struct {
		name string
		hra  bool
	}{
		{"HRA mode", true},
		{"LRA mode", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sk := loadSketch(t, 12, 1, 1000, true, tt.hra)

			rLB, err := sk.RankLowerBound(0.5, WithNumStdDev(1))
			assert.NoError(t, err)
			assert.Greater(t, rLB, 0.0)

			if tt.hra {
				rLB, err = sk.RankLowerBound(995.0/1000, WithNumStdDev(1))
			} else {
				rLB, err = sk.RankLowerBound(5.0/1000, WithNumStdDev(1))
			}
			assert.NoError(t, err)
			assert.Greater(t, rLB, 0.0)

			rUB, err := sk.RankUpperBound(0.5, WithNumStdDev(1))
			assert.NoError(t, err)
			assert.Greater(t, rUB, 0.0)

			if tt.hra {
				rUB, err = sk.RankUpperBound(995.0/1000, WithNumStdDev(1))
			} else {
				rUB, err = sk.RankUpperBound(5.0/1000, WithNumStdDev(1))
			}
			assert.NoError(t, err)
			assert.Greater(t, rUB, 0.0)

			_, err = sk.Ranks([]float32{5, 100})
			assert.NoError(t, err)
		})
	}
}

func TestEmptySketch(t *testing.T) {
	sk, err := NewSketch()
	assert.NoError(t, err)

	_, err = sk.Rank(1)
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.Ranks([]float32{1})
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.Quantile(0.5)
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.Quantiles([]float64{0.5})
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.PMF([]float32{1})
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.CDF([]float32{1}, true)
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.MinItem()
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.MaxItem()
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.QuantileLowerBound(0.5)
	assert.ErrorIs(t, err, ErrEmpty)

	_, err = sk.QuantileUpperBound(0.5)
	assert.ErrorIs(t, err, ErrEmpty)

	items := sk.All()
	assert.Nil(t, items)

	rUB, err := sk.RankUpperBound(0.5, WithNumStdDev(1))
	assert.NoError(t, err)
	assert.Greater(t, rUB, 0.0)
}

func TestSketchSortedView(t *testing.T) {
	t.Run("sketch has two values", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(1))
		assert.NoError(t, sk.Update(2))

		sv, err := sk.SortedView()
		assert.NoError(t, err)
		itr := sv.Iterator()

		// first item
		assert.True(t, itr.Next())

		q, err := itr.Quantile()
		assert.NoError(t, err)
		assert.Equal(t, float32(1), q)

		wt, err := itr.Weight()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), wt)

		natRankExcl, err := itr.NaturalRankWithCriterion(false)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), natRankExcl)

		natRankIncl, err := itr.NaturalRankWithCriterion(true)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), natRankIncl)

		normRankExcl, err := itr.NormalizedRankWithCriterion(false)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, normRankExcl)

		normRankIncl, err := itr.NormalizedRankWithCriterion(true)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, normRankIncl)

		// second item
		assert.True(t, itr.Next())

		q, err = itr.Quantile()
		assert.NoError(t, err)
		assert.Equal(t, float32(2), q)

		wt, err = itr.Weight()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), wt)

		natRankExcl, err = itr.NaturalRankWithCriterion(false)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), natRankExcl)

		natRankIncl, err = itr.NaturalRankWithCriterion(true)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), natRankIncl)

		normRankExcl, err = itr.NormalizedRankWithCriterion(false)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, normRankExcl)

		normRankIncl, err = itr.NormalizedRankWithCriterion(true)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, normRankIncl)
	})

	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)
		sv, err := sk.SortedView()
		assert.NoError(t, err)

		itr := sv.Iterator()
		retainedCount := sk.NumRetained()
		totalN := sk.N()
		count := 0
		var cumWt int64
		for itr.Next() {
			w, err := itr.NaturalRankWithCriterion(true)
			assert.NoError(t, err)
			cumWt = w
			count++
		}
		assert.Equal(t, totalN, cumWt)
		assert.Equal(t, retainedCount, count)
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)
		sv, err := sk.SortedView()
		assert.NoError(t, err)

		itr := sv.Iterator()
		retainedCount := sk.NumRetained()
		totalN := sk.N()
		count := 0
		var cumWt int64
		for itr.Next() {
			w, err := itr.NaturalRankWithCriterion(true)
			assert.NoError(t, err)
			cumWt = w
			count++
		}
		assert.Equal(t, totalN, cumWt)
		assert.Equal(t, retainedCount, count)
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)
		sv, err := sk.SortedView()
		assert.NoError(t, err)

		itr := sv.Iterator()
		retainedCount := sk.NumRetained()
		totalN := sk.N()
		count := 0
		var cumWt int64
		for itr.Next() {
			w, err := itr.NaturalRankWithCriterion(true)
			assert.NoError(t, err)
			cumWt = w
			count++
		}
		assert.Equal(t, totalN, cumWt)
		assert.Equal(t, retainedCount, count)
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)
		sv, err := sk.SortedView()
		assert.NoError(t, err)

		itr := sv.Iterator()
		retainedCount := sk.NumRetained()
		totalN := sk.N()
		count := 0
		var cumWt int64
		for itr.Next() {
			w, err := itr.NaturalRankWithCriterion(true)
			assert.NoError(t, err)
			cumWt = w
			count++
		}
		assert.Equal(t, totalN, cumWt)
		assert.Equal(t, retainedCount, count)
	})
}

func TestSketchString(t *testing.T) {
	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)
		assert.NotEmpty(t, sk.String())
		assert.NotEmpty(t, sk.CompactorDetailString(false))
		assert.NotEmpty(t, sk.CompactorDetailString(true))
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)
		assert.NotEmpty(t, sk.String())
		assert.NotEmpty(t, sk.CompactorDetailString(false))
		assert.NotEmpty(t, sk.CompactorDetailString(true))
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)
		assert.NotEmpty(t, sk.String())
		assert.NotEmpty(t, sk.CompactorDetailString(false))
		assert.NotEmpty(t, sk.CompactorDetailString(true))
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)
		assert.NotEmpty(t, sk.String())
		assert.NotEmpty(t, sk.CompactorDetailString(false))
		assert.NotEmpty(t, sk.CompactorDetailString(true))
	})
}

func TestSketchPMF(t *testing.T) {
	t.Run("NaN", func(t *testing.T) {
		sk, err := NewSketch()
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(1))
		_, err = sk.PMF([]float32{float32(math.NaN())})
		assert.Error(t, err)
	})

	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)
		spArr := []float32{20, 40, 60, 80}
		pmf, err := sk.PMF(spArr)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr)+1, len(pmf))
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)
		spArr := []float32{20, 40, 60, 80}
		pmf, err := sk.PMF(spArr)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr)+1, len(pmf))
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)
		spArr := []float32{20, 40, 60, 80}
		pmf, err := sk.PMF(spArr)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr)+1, len(pmf))
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)
		spArr := []float32{20, 40, 60, 80}
		pmf, err := sk.PMF(spArr)
		assert.NoError(t, err)
		assert.Equal(t, len(spArr)+1, len(pmf))
	})
}

func TestSketchIterator(t *testing.T) {
	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)
		items := sk.All()
		assert.NotEmpty(t, items)
		for _, item := range items {
			assert.Greater(t, item.Weight, int64(0))
		}
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)
		items := sk.All()
		assert.NotEmpty(t, items)
		for _, item := range items {
			assert.Greater(t, item.Weight, int64(0))
		}
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)
		items := sk.All()
		assert.NotEmpty(t, items)
		for _, item := range items {
			assert.Greater(t, item.Weight, int64(0))
		}
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)
		items := sk.All()
		assert.NotEmpty(t, items)
		for _, item := range items {
			assert.Greater(t, item.Weight, int64(0))
		}
	})
}

func TestSketchReset(t *testing.T) {
	t.Run("k=20 up HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, true)
		sk.Reset()
		assert.True(t, sk.IsEmpty())
	})

	t.Run("k=20 down LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, false)
		sk.Reset()
		assert.True(t, sk.IsEmpty())
	})

	t.Run("k=20 down HRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, false, true)
		sk.Reset()
		assert.True(t, sk.IsEmpty())
	})

	t.Run("k=20 up LRA", func(t *testing.T) {
		sk := loadSketch(t, 20, 1, 100, true, false)
		sk.Reset()
		assert.True(t, sk.IsEmpty())
	})
}

func TestSketchMinMaxItem(t *testing.T) {
	t.Run("sequential values", func(t *testing.T) {
		sk := loadSketch(t, 12, 1, 100, true, true)
		minV, err := sk.MinItem()
		assert.NoError(t, err)
		assert.Equal(t, float32(1), minV)

		maxV, err := sk.MaxItem()
		assert.NoError(t, err)
		assert.Equal(t, float32(100), maxV)
	})

	t.Run("reverse order", func(t *testing.T) {
		sk := loadSketch(t, 12, 1, 100, false, true)
		minV, err := sk.MinItem()
		assert.NoError(t, err)
		assert.Equal(t, float32(1), minV)

		maxV, err := sk.MaxItem()
		assert.NoError(t, err)
		assert.Equal(t, float32(100), maxV)
	})
}

func TestSketchQuantileBounds(t *testing.T) {
	t.Run("QuantileLowerBound", func(t *testing.T) {
		sk := loadSketch(t, 12, 1, 1000, true, true)
		qlb, err := sk.QuantileLowerBound(0.5, WithNumStdDev(1))
		assert.NoError(t, err)
		assert.Greater(t, qlb, float32(0))
	})

	t.Run("QuantileUpperBound", func(t *testing.T) {
		sk := loadSketch(t, 12, 1, 1000, true, true)
		qub, err := sk.QuantileUpperBound(0.5, WithNumStdDev(1))
		assert.NoError(t, err)
		assert.Greater(t, qub, float32(0))
	})

	t.Run("bounds ordering", func(t *testing.T) {
		sk := loadSketch(t, 12, 1, 1000, true, true)
		qlb, err := sk.QuantileLowerBound(0.5, WithNumStdDev(2))
		assert.NoError(t, err)
		qub, err := sk.QuantileUpperBound(0.5, WithNumStdDev(2))
		assert.NoError(t, err)
		assert.LessOrEqual(t, qlb, qub)
	})
}
