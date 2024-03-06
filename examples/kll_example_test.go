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

package examples

import (
	"fmt"
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/kll"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKllItemsSketch(t *testing.T) {
	// Create a new KLL sketch
	sketch, err := kll.NewKllItemsSketchWithDefault[string](common.ArrayOfStringsSerDe{})
	assert.NoError(t, err)

	// Update the sketch with 1000 items
	for i := 0; i < 1000; i++ {
		sketch.Update(fmt.Sprintf("item_%d", i))
	}

	// Get the quantiles
	quantiles := []float64{0.0, 0.5, 1.0}
	values, err := sketch.GetQuantiles(quantiles, true)
	assert.NoError(t, err)

	// Validate the quantiles
	assert.Equal(t, "item_0", values[0])
	assert.Equal(t, "item_548", values[1])
	assert.Equal(t, "item_999", values[2])

	// Get the PMF
	pmf, err := sketch.GetPMF([]string{"item_0", "item_548", "item_999"}, true)
	assert.NoError(t, err)

	// Validate the PMF
	assert.Equal(t, 0.004, pmf[0])
	assert.Equal(t, 0.498, pmf[1])
	assert.Equal(t, 0.498, pmf[2])

	// Get the CDF
	cdf, err := sketch.GetCDF([]string{"item_0", "item_548", "item_999"}, true)
	assert.NoError(t, err)

	// Validate the CDF
	assert.Equal(t, 0.004, cdf[0])
	assert.Equal(t, 0.502, cdf[1])
	assert.Equal(t, 1.0, cdf[2])

	// Get the rank of an item
	rank, err := sketch.GetRank("item_548", true)
	assert.NoError(t, err)
	assert.Equal(t, 0.502, rank)
}
