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

func TestItemsSketchKLimits(t *testing.T) {
	_, err := NewItemsSketch[string](uint16(_MIN_K), stringItemsSketchOp{})
	assert.NoError(t, err)
	_, err = NewItemsSketch[string](uint16(_MAX_K), stringItemsSketchOp{})
	assert.NoError(t, err)
	_, err = NewItemsSketch[string](uint16(_MIN_K-1), stringItemsSketchOp{})
	assert.Error(t, err)
}

func TestItemsSketchEmpty(t *testing.T) {
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
	_, err = sketch.GetPMF(splitPoints, 1, true)
	assert.Error(t, err)
	_, err = sketch.GetCDF(splitPoints, 1, true)
	assert.Error(t, err)
}

func TestItemsSketchBadQuantile(t *testing.T) {
	sketch, err := NewItemsSketch[string](200, stringItemsSketchOp{})
	assert.NoError(t, err)
	sketch.Update("") // has to be non-empty to reach the check
	_, err = sketch.GetQuantile(-1, true)
	assert.Error(t, err)
}

func TestItemsSketchOneValue(t *testing.T) {
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

func TestItemsSketchTenValues(t *testing.T) {
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
