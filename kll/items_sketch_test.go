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
	"github.com/stretchr/testify/assert"
	"testing"
)

type floatItemsSketchOp struct {
}

func (f floatItemsSketchOp) identity() float64 {
	return 0
}

func (f floatItemsSketchOp) lessFn(list []float64) lessFn[float64] {
	return func(a int, b int) bool {
		return list[a] < list[b]
	}
}

func TestItemsSketchKLimits(t *testing.T) {
	_, err := NewItemsSketch[float64](uint16(_MIN_K), floatItemsSketchOp{})
	assert.NoError(t, err)
	_, err = NewItemsSketch[float64](uint16(_MAX_K), floatItemsSketchOp{})
	assert.NoError(t, err)
	_, err = NewItemsSketch[float64](uint16(_MIN_K-1), floatItemsSketchOp{})
	assert.Error(t, err)
}

/*
  SECTION("empty") {
    kll_float_sketch sketch(200, std::less<float>(), 0);
    REQUIRE(sketch.is_empty());
    REQUIRE_FALSE(sketch.is_estimation_mode());
    REQUIRE(sketch.get_n() == 0);
    REQUIRE(sketch.get_num_retained() == 0);
    REQUIRE_THROWS_AS(sketch.get_min_item(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_max_item(), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_rank(0), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_quantile(0.5), std::runtime_error);
    const float split_points[1] {0};
    REQUIRE_THROWS_AS(sketch.get_PMF(split_points, 1), std::runtime_error);
    REQUIRE_THROWS_AS(sketch.get_CDF(split_points, 1), std::runtime_error);

    for (auto pair: sketch) {
      unused(pair); // to suppress "unused" warning
      FAIL("should be no iterations over an empty sketch");
    }
  }
*/

func TestItemsSketchEmpty(t *testing.T) {
	sketch, err := NewItemsSketch[float64](200, floatItemsSketchOp{})
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.False(t, sketch.IsEstimationMode())
	assert.Equal(t, uint64(0), sketch.GetN())
	assert.Equal(t, uint32(0), sketch.GetNumRetained())
	_, err = sketch.GetMinItem()
	assert.Error(t, err)
	_, err = sketch.GetMaxItem()
	assert.Error(t, err)
	_, err = sketch.GetRank(0, true)
	assert.Error(t, err)
	_, err = sketch.GetQuantile(0.5, true)
	assert.Error(t, err)
	splitPoints := []float64{0}
	_, err = sketch.GetPMF(splitPoints, 1, true)
	assert.Error(t, err)
}
