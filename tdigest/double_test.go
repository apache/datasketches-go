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

package tdigest

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDouble(t *testing.T) {
	t.Run("Default K", func(t *testing.T) {
		sketch, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.NotNil(t, sketch)
		assert.Equal(t, uint16(DefaultK), sketch.K())
		assert.True(t, sketch.IsEmpty())
	})

	t.Run("Custom K", func(t *testing.T) {
		sketch, err := NewDouble(100)
		assert.NoError(t, err)
		assert.NotNil(t, sketch)
		assert.Equal(t, uint16(100), sketch.K())
	})

	t.Run("Minimum Valid K", func(t *testing.T) {
		sketch, err := NewDouble(10)
		assert.NoError(t, err)
		assert.NotNil(t, sketch)
		assert.Equal(t, uint16(10), sketch.K())
	})

	t.Run("Small K With Fudge", func(t *testing.T) {
		sketch, err := NewDouble(20)
		assert.NoError(t, err)
		assert.NotNil(t, sketch)
		assert.Equal(t, uint16(20), sketch.K())
	})

	t.Run("Invalid K Too Small", func(t *testing.T) {
		_, err := NewDouble(9)
		assert.ErrorIs(t, err, ErrInvalidK)
	})

	t.Run("Invalid K Zero", func(t *testing.T) {
		_, err := NewDouble(0)
		assert.ErrorIs(t, err, ErrInvalidK)
	})
}

func TestDouble_Update(t *testing.T) {
	t.Run("Single Value", func(t *testing.T) {
		sketch, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sketch.Update(1.0)
		assert.NoError(t, err)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint64(1), sketch.TotalWeight())
	})

	t.Run("Multiple Values", func(t *testing.T) {
		sketch, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			err = sketch.Update(float64(i))
			assert.NoError(t, err)
		}
		assert.Equal(t, uint64(100), sketch.TotalWeight())
	})

	t.Run("NaN Returns Error", func(t *testing.T) {
		sketch, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sketch.Update(math.NaN())
		assert.ErrorIs(t, err, ErrNaN)
		assert.True(t, sketch.IsEmpty())
		assert.Equal(t, uint64(0), sketch.TotalWeight())
	})

	t.Run("Positive Infinity Returns Error", func(t *testing.T) {
		sketch, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sketch.Update(math.Inf(1))
		assert.ErrorIs(t, err, ErrInfinity)
		assert.True(t, sketch.IsEmpty())
		assert.Equal(t, uint64(0), sketch.TotalWeight())
	})

	t.Run("Negative Infinity Returns Error", func(t *testing.T) {
		sketch, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sketch.Update(math.Inf(-1))
		assert.ErrorIs(t, err, ErrInfinity)
		assert.True(t, sketch.IsEmpty())
		assert.Equal(t, uint64(0), sketch.TotalWeight())
	})

	t.Run("Triggers Compression", func(t *testing.T) {
		sketch, err := NewDouble(10)
		assert.NoError(t, err)

		// Add enough values to trigger compression
		for i := 0; i < 500; i++ {
			err = sketch.Update(float64(i))
			assert.NoError(t, err)
		}
		assert.Equal(t, uint64(500), sketch.TotalWeight())
	})

	t.Run("Min Max Tracking", func(t *testing.T) {
		sketch, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sketch.Update(5.0)
		assert.NoError(t, err)
		err = sketch.Update(1.0)
		assert.NoError(t, err)
		err = sketch.Update(10.0)
		assert.NoError(t, err)

		minVal, err := sketch.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, 1.0, minVal)

		maxVal, err := sketch.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, 10.0, maxVal)
	})
}

func TestDouble_Merge(t *testing.T) {
	t.Run("Merge Empty Into Non-Empty", func(t *testing.T) {
		sk1, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		sk2, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 50; i++ {
			err = sk1.Update(float64(i))
			assert.NoError(t, err)
		}

		err = sk1.Merge(sk2)
		assert.ErrorIs(t, err, ErrEmpty)
		assert.Equal(t, uint64(50), sk1.TotalWeight())
	})

	t.Run("Merge Non-Empty Into Empty", func(t *testing.T) {
		sk1, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		sk2, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 50; i++ {
			err = sk2.Update(float64(i))
			assert.NoError(t, err)
		}

		err = sk1.Merge(sk2)
		assert.NoError(t, err)
		assert.Equal(t, uint64(50), sk1.TotalWeight())
	})

	t.Run("Merge Two Empty", func(t *testing.T) {
		sk1, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		sk2, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk1.Merge(sk2)
		assert.ErrorIs(t, err, ErrEmpty)
		assert.True(t, sk1.IsEmpty())
	})

	t.Run("Merge Small", func(t *testing.T) {
		sk1, err := NewDouble(10)
		assert.NoError(t, err)
		err = sk1.Update(1.0)
		assert.NoError(t, err)
		err = sk1.Update(2.0)
		assert.NoError(t, err)

		sk2, err := NewDouble(10)
		assert.NoError(t, err)
		err = sk2.Update(2.0)
		assert.NoError(t, err)
		err = sk2.Update(3.0)
		assert.NoError(t, err)

		err = sk1.Merge(sk2)
		assert.NoError(t, err)

		minVal, err := sk1.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, 1.0, minVal)

		maxVal, err := sk1.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, 3.0, maxVal)

		assert.Equal(t, uint64(4), sk1.TotalWeight())

		rank, err := sk1.Rank(0.99)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), rank)

		rank, err = sk1.Rank(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.125, rank)

		rank, err = sk1.Rank(2)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, rank)

		rank, err = sk1.Rank(3)
		assert.NoError(t, err)
		assert.Equal(t, 0.875, rank)

		rank, err = sk1.Rank(3.01)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, rank)
	})

	t.Run("Merge Large", func(t *testing.T) {
		sk1, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		sk2, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		n := 10000
		for i := 0; i < n/2; i++ {
			err = sk1.Update(float64(i))
			assert.NoError(t, err)
			err = sk2.Update(float64(n)/2.0 + float64(i))
			assert.NoError(t, err)
		}

		err = sk1.Merge(sk2)
		assert.NoError(t, err)

		assert.Equal(t, uint64(n), sk1.TotalWeight())

		minVal, err := sk1.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(0), minVal)

		maxVal, err := sk1.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(n-1), maxVal)

		rank, err := sk1.Rank(0)
		assert.NoError(t, err)
		assert.InDelta(t, 0, rank, 0.0001)

		rank, err = sk1.Rank(float64(n) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.25, rank, 0.0001)

		rank, err = sk1.Rank(float64(n) / 2.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.5, rank, 0.0001)

		rank, err = sk1.Rank(float64(n*3) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.75, rank, 0.0001)

		rank, err = sk1.Rank(float64(n))
		assert.NoError(t, err)
		assert.Equal(t, float64(1), rank)
	})
}

func TestDouble_IsEmpty(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.True(t, sk.IsEmpty())
	})

	t.Run("Not Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		assert.False(t, sk.IsEmpty())
	})
}

func TestDouble_MinValue(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		_, err = sk.MinValue()
		assert.ErrorIs(t, err, ErrEmpty)
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(42.0)
		assert.NoError(t, err)
		minVal, err := sk.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, 42.0, minVal)
	})

	t.Run("Multiple Values", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(5.0)
		assert.NoError(t, err)
		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(10.0)
		assert.NoError(t, err)

		minVal, err := sk.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, 1.0, minVal)
	})
}

func TestDouble_MaxValue(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		_, err = sk.MaxValue()
		assert.ErrorIs(t, err, ErrEmpty)
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(42.0)
		assert.NoError(t, err)
		maxVal, err := sk.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, 42.0, maxVal)
	})

	t.Run("Multiple Values", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(5.0)
		assert.NoError(t, err)
		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(10.0)
		assert.NoError(t, err)

		maxVal, err := sk.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, 10.0, maxVal)
	})
}

func TestDouble_TotalWeight(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), sk.TotalWeight())
	})

	t.Run("After Updates", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}
		assert.Equal(t, uint64(100), sk.TotalWeight())
	})

	t.Run("After Merge", func(t *testing.T) {
		sk1, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		sk2, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 50; i++ {
			err = sk1.Update(float64(i))
			assert.NoError(t, err)
			err = sk2.Update(float64(i + 50))
			assert.NoError(t, err)
		}

		err = sk1.Merge(sk2)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), sk1.TotalWeight())
	})
}

func TestDouble_Rank(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		_, err = sk.Rank(0.5)
		assert.ErrorIs(t, err, ErrEmpty)
	})

	t.Run("NaN Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		_, err = sk.Rank(math.NaN())
		assert.ErrorIs(t, err, ErrNaN)
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(5.0)
		assert.NoError(t, err)

		rank, err := sk.Rank(5.0)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, rank)
	})

	t.Run("Value Below Min", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(10.0)
		assert.NoError(t, err)
		err = sk.Update(20.0)
		assert.NoError(t, err)

		rank, err := sk.Rank(5.0)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, rank)
	})

	t.Run("Value Above Max", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(10.0)
		assert.NoError(t, err)
		err = sk.Update(20.0)
		assert.NoError(t, err)

		rank, err := sk.Rank(25.0)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, rank)
	})

	t.Run("Uniform Distribution", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		rank, err := sk.Rank(50.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.5, rank, 0.1)

		rank, err = sk.Rank(25.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.25, rank, 0.1)

		rank, err = sk.Rank(75.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.75, rank, 0.1)
	})

	t.Run("At Min Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(2.0)
		assert.NoError(t, err)
		err = sk.Update(3.0)
		assert.NoError(t, err)

		rank, err := sk.Rank(1.0)
		assert.NoError(t, err)
		assert.Greater(t, rank, 0.0)
		assert.Less(t, rank, 0.5)
	})

	t.Run("At Max Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(2.0)
		assert.NoError(t, err)
		err = sk.Update(3.0)
		assert.NoError(t, err)

		rank, err := sk.Rank(3.0)
		assert.NoError(t, err)
		assert.Greater(t, rank, 0.5)
		assert.LessOrEqual(t, rank, 1.0)
	})

	t.Run("Two Values", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(2.0)
		assert.NoError(t, err)

		rank, err := sk.Rank(0.99)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), rank)

		rank, err = sk.Rank(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.25, rank)

		rank, err = sk.Rank(1.25)
		assert.NoError(t, err)
		assert.Equal(t, 0.375, rank)

		rank, err = sk.Rank(1.5)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, rank)

		rank, err = sk.Rank(1.75)
		assert.NoError(t, err)
		assert.Equal(t, 0.625, rank)

		rank, err = sk.Rank(2)
		assert.NoError(t, err)
		assert.Equal(t, 0.75, rank)

		rank, err = sk.Rank(2.01)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, rank)
	})

	t.Run("Repeated Values", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(1.0)
		assert.NoError(t, err)

		rank, err := sk.Rank(0.99)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), rank)

		rank, err = sk.Rank(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, rank)

		rank, err = sk.Rank(1.01)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, rank)
	})

	t.Run("Repeated Block", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		err = sk.Update(2.0)
		assert.NoError(t, err)
		err = sk.Update(2.0)
		assert.NoError(t, err)
		err = sk.Update(3.0)
		assert.NoError(t, err)

		rank, err := sk.Rank(0.99)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), rank)

		rank, err = sk.Rank(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.125, rank)

		rank, err = sk.Rank(2)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, rank)

		rank, err = sk.Rank(3)
		assert.NoError(t, err)
		assert.Equal(t, 0.875, rank)

		rank, err = sk.Rank(3.01)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, rank)
	})
}

func TestDouble_Quantile(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		_, err = sk.Quantile(0.5)
		assert.ErrorIs(t, err, ErrEmpty)
	})

	t.Run("Invalid Rank Below Zero", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		_, err = sk.Quantile(-0.1)
		assert.ErrorIs(t, err, ErrInvalidRank)
	})

	t.Run("Invalid Rank Above One", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		_, err = sk.Quantile(1.1)
		assert.ErrorIs(t, err, ErrInvalidRank)
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(42.0)
		assert.NoError(t, err)

		q, err := sk.Quantile(0.0)
		assert.NoError(t, err)
		assert.Equal(t, 42.0, q)

		q, err = sk.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, 42.0, q)

		q, err = sk.Quantile(1.0)
		assert.NoError(t, err)
		assert.Equal(t, 42.0, q)
	})

	t.Run("Rank Zero Returns Min", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		q, err := sk.Quantile(0.0)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, q)
	})

	t.Run("Rank One Returns Max", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		q, err := sk.Quantile(1.0)
		assert.NoError(t, err)
		assert.Equal(t, 100.0, q)
	})

	t.Run("Median Of Uniform Distribution", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		q, err := sk.Quantile(0.5)
		assert.NoError(t, err)
		assert.InDelta(t, 50.0, q, 5.0)
	})

	t.Run("Quartiles", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		q25, err := sk.Quantile(0.25)
		assert.NoError(t, err)
		assert.InDelta(t, 25.0, q25, 5.0)

		q75, err := sk.Quantile(0.75)
		assert.NoError(t, err)
		assert.InDelta(t, 75.0, q75, 5.0)
	})

	t.Run("Multiple Centroids", func(t *testing.T) {
		sk, err := NewDouble(50)
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		q, err := sk.Quantile(0.5)
		assert.NoError(t, err)
		assert.InDelta(t, 500.0, q, 100.0)
	})
}

func TestDouble_PMF(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		_, err = sk.PMF([]float64{0.5})
		assert.ErrorIs(t, err, ErrEmpty)
	})

	t.Run("Invalid Split Points NaN", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		_, err = sk.PMF([]float64{math.NaN()})
		assert.ErrorIs(t, err, errNanInSplitPoints)
	})

	t.Run("Invalid Split Points Not Increasing", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		_, err = sk.PMF([]float64{5.0, 3.0})
		assert.ErrorIs(t, err, errInvalidSplitPoints)
	})

	t.Run("Single Split Point", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		pmf, err := sk.PMF([]float64{50.0})
		assert.NoError(t, err)
		assert.Len(t, pmf, 2)

		// Sum of PMF should be 1
		sum := 0.0
		for _, p := range pmf {
			sum += p
		}
		assert.InDelta(t, 1.0, sum, 0.001)
	})

	t.Run("Multiple Split Points", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		pmf, err := sk.PMF([]float64{25.0, 50.0, 75.0})
		assert.NoError(t, err)
		assert.Len(t, pmf, 4)

		// Sum of PMF should be 1
		sum := 0.0
		for _, p := range pmf {
			sum += p
		}
		assert.InDelta(t, 1.0, sum, 0.001)
	})
}

func TestDouble_CDF(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		_, err = sk.CDF([]float64{0.5})
		assert.ErrorIs(t, err, ErrEmpty)
	})

	t.Run("Invalid Split Points NaN", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		_, err = sk.CDF([]float64{math.NaN()})
		assert.ErrorIs(t, err, errNanInSplitPoints)
	})

	t.Run("Invalid Split Points Not Increasing", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(1.0)
		assert.NoError(t, err)
		_, err = sk.CDF([]float64{5.0, 3.0})
		assert.ErrorIs(t, err, errInvalidSplitPoints)
	})

	t.Run("Single Split Point", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		cdf, err := sk.CDF([]float64{50.0})
		assert.NoError(t, err)
		assert.Len(t, cdf, 2)
		assert.InDelta(t, 0.5, cdf[0], 0.1)
		assert.Equal(t, 1.0, cdf[1])
	})

	t.Run("Multiple Split Points", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		cdf, err := sk.CDF([]float64{25.0, 50.0, 75.0})
		assert.NoError(t, err)
		assert.Len(t, cdf, 4)

		// CDF should be monotonically increasing
		for i := 1; i < len(cdf); i++ {
			assert.GreaterOrEqual(t, cdf[i], cdf[i-1])
		}

		// Last value should be 1
		assert.Equal(t, 1.0, cdf[len(cdf)-1])
	})

	t.Run("CDF Values In Range", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 1; i <= 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		cdf, err := sk.CDF([]float64{25.0, 50.0, 75.0})
		assert.NoError(t, err)

		for _, v := range cdf {
			assert.GreaterOrEqual(t, v, 0.0)
			assert.LessOrEqual(t, v, 1.0)
		}
	})
}

func TestDouble_String(t *testing.T) {
	t.Run("Empty Without Centroids", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		result := sk.String(false)
		assert.Contains(t, result, "### t-Digest summary:")
		assert.Contains(t, result, "Nominal k")
		assert.Contains(t, result, "Centroids          : 0")
		assert.Contains(t, result, "Buffered           : 0")
		assert.Contains(t, result, "### End t-Digest summary")
		assert.NotContains(t, result, "Centroids:")
	})

	t.Run("Non-Empty Without Centroids", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		result := sk.String(false)
		assert.Contains(t, result, "### t-Digest summary:")
		assert.Contains(t, result, "Total Weight       : 10")
		assert.Contains(t, result, "Min")
		assert.Contains(t, result, "Max")
		assert.NotContains(t, result, "Centroids:")
	})

	t.Run("With Centroids", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		result := sk.String(true)
		assert.Contains(t, result, "### t-Digest summary:")
		assert.Contains(t, result, "Buffer:")
	})
}

func TestDouble_SerializedSizeBytes(t *testing.T) {
	t.Run("Empty Without Buffer", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		size := sk.SerializedSizeBytes(false)
		assert.Greater(t, size, 0)
	})

	t.Run("Empty With Buffer", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		sizeWithBuffer := sk.SerializedSizeBytes(true)
		sizeWithoutBuffer := sk.SerializedSizeBytes(false)
		assert.GreaterOrEqual(t, sizeWithBuffer, sizeWithoutBuffer)
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		err = sk.Update(42.0)
		assert.NoError(t, err)
		size := sk.SerializedSizeBytes(false)
		assert.Greater(t, size, 0)
	})

	t.Run("Multiple Values Without Buffer", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		size := sk.SerializedSizeBytes(false)
		assert.Greater(t, size, 0)
	})

	t.Run("Multiple Values With Buffer", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			err = sk.Update(float64(i))
			assert.NoError(t, err)
		}

		// With buffer: preamble(16) + min/max(16) + centroids(0) + buffer(8*100) = 832
		sizeWithBuffer := sk.SerializedSizeBytes(true)
		assert.Equal(t, 832, sizeWithBuffer)

		// Without buffer compresses first: preamble(16) + min/max(16) + centroids(16*100) = 1632
		sizeWithoutBuffer := sk.SerializedSizeBytes(false)
		assert.Equal(t, 1632, sizeWithoutBuffer)
	})
}
