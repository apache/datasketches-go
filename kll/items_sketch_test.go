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

package kll

import (
	"github.com/apache/datasketches-go/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

type stringItemsSketchOp struct {
}

func (f stringItemsSketchOp) identity() string {
	return ""
}

func (f stringItemsSketchOp) lessFn() common.LessFn[string] {
	return func(a string, b string) bool {
		return a < b
	}
}

const (
	PMF_EPS_FOR_K_256       = 0.013 // PMF rank error (epsilon) for k=256
	NUMERIC_NOISE_TOLERANCE = 1e-6
)

func TestItemsSketch_KLimits(t *testing.T) {
	_, err := NewItemsSketch[string](uint16(_MIN_K), stringItemsSketchOp{})
	assert.NoError(t, err)
	_, err = NewItemsSketch[string](uint16(_MAX_K), stringItemsSketchOp{})
	assert.NoError(t, err)
	_, err = NewItemsSketch[string](uint16(_MIN_K-1), stringItemsSketchOp{})
	assert.Error(t, err)
}

func TestItemsSketch_Empty(t *testing.T) {
	sketch, err := NewItemsSketch[string](200, stringItemsSketchOp{})
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.False(t, sketch.IsEstimationMode())
	assert.Equal(t, uint64(0), sketch.GetN())
	assert.Equal(t, uint32(0), sketch.GetNumRetained())
	_, err = sketch.GetMinItem()
	assert.Error(t, err)
	_, err = sketch.GetMaxItem()
	assert.Error(t, err)
	_, err = sketch.GetRank("", true)
	assert.Error(t, err)
	_, err = sketch.GetQuantile(0.5, true)
	assert.Error(t, err)
	splitPoints := []string{""}
	_, err = sketch.GetPMF(splitPoints, true)
	assert.Error(t, err)
	_, err = sketch.GetCDF(splitPoints, true)
	assert.Error(t, err)
}

func TestItemsSketch_BadQuantile(t *testing.T) {
	sketch, err := NewItemsSketch[string](200, stringItemsSketchOp{})
	assert.NoError(t, err)
	sketch.Update("") // has to be non-empty to reach the check
	_, err = sketch.GetQuantile(-1, true)
	assert.Error(t, err)
}

func TestItemsSketch_OneValue(t *testing.T) {
	sketch, err := NewItemsSketch[string](200, stringItemsSketchOp{})
	assert.NoError(t, err)
	sketch.Update("A")
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint64(1), sketch.GetN())
	assert.Equal(t, uint32(1), sketch.GetNumRetained())
	v, err := sketch.GetRank("A", false)
	assert.Equal(t, float64(0), v)
	v, err = sketch.GetRank("B", false)
	assert.Equal(t, float64(1), v)
	v, err = sketch.GetRank("A", false)
	assert.Equal(t, float64(0), v)
	v, err = sketch.GetRank("B", false)
	assert.Equal(t, float64(1), v)
	v, err = sketch.GetRank("@", true)
	assert.Equal(t, float64(0), v)
	v, err = sketch.GetRank("A", true)
	assert.Equal(t, float64(1), v)
	s, err := sketch.GetMinItem()
	assert.Equal(t, "A", s)
	s, err = sketch.GetMaxItem()
	assert.Equal(t, "A", s)
	s, err = sketch.GetQuantile(0.5, false)
	assert.Equal(t, "A", s)
	s, err = sketch.GetQuantile(0.5, true)
	assert.Equal(t, "A", s)
}

func TestItemsSketch_TenValues(t *testing.T) {
	tenStr := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	sketch, err := NewItemsSketch[string](20, stringItemsSketchOp{})
	assert.NoError(t, err)
	strLen := len(tenStr)
	dblStrLen := float64(strLen)
	for i := 1; i <= strLen; i++ {
		sketch.Update(tenStr[i-1])
	}
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint64(strLen), sketch.GetN())
	assert.Equal(t, uint32(strLen), sketch.GetNumRetained())
	for i := 1; i <= strLen; i++ {
		v, err := sketch.GetRank(tenStr[i-1], false)
		assert.Equal(t, float64(i-1)/dblStrLen, v, "i: %d", i)
		assert.NoError(t, err, "i: %d", i)
		v, err = sketch.GetRank(tenStr[i-1], true)
		assert.Equal(t, float64(i)/dblStrLen, v)
		assert.NoError(t, err)
	}
	qArr := tenStr
	rOut, err := sketch.GetRanks(qArr, true) //inclusive
	assert.NoError(t, err)
	for i := 0; i < len(qArr); i++ {
		assert.Equal(t, float64(i+1)/dblStrLen, rOut[i])
	}
	rOut, err = sketch.GetRanks(qArr, false) //exclusive
	assert.NoError(t, err)
	for i := 0; i < len(qArr); i++ {
		assert.Equal(t, float64(i)/dblStrLen, rOut[i])
	}

	for i := 0; i <= strLen; i++ {
		rank := float64(i) / dblStrLen
		var q string
		if rank == 1.0 {
			q = tenStr[i-1]
		} else {
			q = tenStr[i]
		}
		s, err := sketch.GetQuantile(rank, false)
		assert.Equal(t, q, s, "i: %d", i)
		assert.NoError(t, err)
		if rank == 0 {
			q = tenStr[i]
		} else {
			q = tenStr[i-1]
		}
		s, err = sketch.GetQuantile(rank, true)
		assert.Equal(t, q, s)
		assert.NoError(t, err)
	}

	{
		// getQuantile() and getQuantiles() equivalence EXCLUSIVE
		quantiles, err := sketch.GetQuantiles([]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, false)
		assert.NoError(t, err)
		for i := 0; i <= 10; i++ {
			q, err := sketch.GetQuantile(float64(i)/10.0, false)
			assert.NoError(t, err)
			assert.Equal(t, q, quantiles[i])
		}
	}

	{
		// getQuantile() and getQuantiles() equivalence INCLUSIVE
		quantiles, err := sketch.GetQuantiles([]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, true)
		assert.NoError(t, err)
		for i := 0; i <= 10; i++ {
			q, err := sketch.GetQuantile(float64(i)/10.0, true)
			assert.NoError(t, err)
			assert.Equal(t, q, quantiles[i])
		}
	}
}

func TestItemsSketch_ManyValuesEstimationMode(T *testing.T) {
	sketch, err := NewItemsSketch[string](_DEFAULT_K, stringItemsSketchOp{})
	assert.NoError(T, err)
	n := 1_000_000
	digits := numDigits(n)

	for i := 1; i <= n; i++ {
		// i == 201
		sketch.Update(intToFixedLengthString(i, digits))
		assert.Equal(T, uint64(i), sketch.GetN())
	}

	lastItem := sketch.items[len(sketch.items)-1]
	assert.NotNil(T, lastItem)
	// test getRank
	for i := 1; i <= 1; i++ {
		trueRank := float64(i) / float64(n)
		s := intToFixedLengthString(i, digits)
		r, err := sketch.GetRank(s, true)
		assert.InDelta(T, trueRank, r, PMF_EPS_FOR_K_256)
		assert.NoError(T, err)
	}

	s := intToFixedLengthString(n/2, digits)
	pmf, err := sketch.GetPMF([]string{s}, true) // split at median
	assert.NoError(T, err)
	assert.Equal(T, 2, len(pmf))
	assert.InDelta(T, 0.5, pmf[0], PMF_EPS_FOR_K_256)
	assert.InDelta(T, 0.5, pmf[1], PMF_EPS_FOR_K_256)

	minV, err := sketch.GetMinItem()
	assert.NoError(T, err)
	assert.Equal(T, intToFixedLengthString(1, digits), minV)

	maxV, err := sketch.GetMaxItem()
	assert.NoError(T, err)
	assert.Equal(T, intToFixedLengthString(n, digits), maxV)

	// check at every 0.1 percentage point
	fractions := make([]float64, 1001)
	reverseFractions := make([]float64, 1001) // check that ordering doesn't matter
	for i := 0; i <= 1000; i++ {
		fractions[i] = float64(i) / 1000.0
		reverseFractions[1000-i] = fractions[i]
	}
	quantiles, err := sketch.GetQuantiles(fractions, true)
	assert.NoError(T, err)
	reverseQuantiles, err := sketch.GetQuantiles(reverseFractions, true)
	assert.NoError(T, err)
	previousQuantile := ""
	for i := 0; i <= 1000; i++ {
		quantile, err := sketch.GetQuantile(fractions[i], true)
		assert.NoError(T, err)
		assert.Equal(T, quantile, quantiles[i])
		assert.Equal(T, quantile, reverseQuantiles[1000-i])
		assert.True(T, previousQuantile <= quantile)
		previousQuantile = quantile
	}
}

func TestItemsSketch_GetRankGetCdfGetPmfConsistency(t *testing.T) {
	sketch, err := NewItemsSketch[string](_DEFAULT_K, stringItemsSketchOp{})
	assert.NoError(t, err)
	n := 1000
	digits := numDigits(n)
	quantiles := make([]string, n)
	for i := 0; i < n; i++ {
		str := intToFixedLengthString(i, digits)
		sketch.Update(str)
		quantiles[i] = str
	}
	{ //EXCLUSIVE
		ranks, err := sketch.GetCDF(quantiles, false)
		assert.NoError(t, err)
		pmf, err := sketch.GetPMF(quantiles, false)
		assert.NoError(t, err)
		sumPmf := 0.0
		for i := 0; i < n; i++ {
			r, err := sketch.GetRank(quantiles[i], false)
			assert.NoError(t, err)
			assert.InDelta(t, ranks[i], r, NUMERIC_NOISE_TOLERANCE)
			sumPmf += pmf[i]
			assert.InDelta(t, ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE)
		}
		sumPmf += pmf[n]
		assert.InDelta(t, sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE)
		assert.InDelta(t, ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE)
	}
	{ // INCLUSIVE (default)
		ranks, err := sketch.GetCDF(quantiles, true)
		assert.NoError(t, err)
		pmf, err := sketch.GetPMF(quantiles, true)
		assert.NoError(t, err)
		sumPmf := 0.0
		for i := 0; i < n; i++ {
			r, err := sketch.GetRank(quantiles[i], true)
			assert.NoError(t, err)
			assert.InDelta(t, ranks[i], r, NUMERIC_NOISE_TOLERANCE)
			sumPmf += pmf[i]
			assert.InDelta(t, ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE)
		}
		sumPmf += pmf[n]
		assert.InDelta(t, sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE)
		assert.InDelta(t, ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE)
	}
}
