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
)

func TestIncludeMinMax(t *testing.T) {
	t.Run("Adjust Both Ends", func(t *testing.T) {
		quantiles := []float32{2, 4, 6, 7}
		cumWeights := []int64{2, 4, 6, 8}

		got := IncludeMinMax(quantiles, cumWeights, 8, 1)

		wantQuantiles := []float32{1, 2, 4, 6, 7, 8}
		wantCumWeights := []int64{1, 2, 4, 6, 7, 8}

		assert.Equal(t, wantQuantiles, got.Quantiles)
		assert.Equal(t, wantCumWeights, got.CumWeights)
	})

	t.Run("Return original slices", func(t *testing.T) {
		quantiles := []float32{2, 4, 6, 7}
		cumWeights := []int64{2, 4, 6, 8}

		got := IncludeMinMax(quantiles, cumWeights, 7, 2)

		assert.Equal(t, quantiles, got.Quantiles)
		assert.Equal(t, cumWeights, got.CumWeights)
	})

	t.Run("Adjust Low End Only", func(t *testing.T) {
		quantiles := []float32{2, 4, 6, 8}
		cumWeights := []int64{2, 4, 6, 8}

		got := IncludeMinMax(quantiles, cumWeights, 8, 1)

		wantQuantiles := []float32{1, 2, 4, 6, 8}
		wantCumWeights := []int64{1, 2, 4, 6, 8}

		assert.Equal(t, wantQuantiles, got.Quantiles)
		assert.Equal(t, wantCumWeights, got.CumWeights)
	})

	t.Run("Adjust High End Only", func(t *testing.T) {
		quantiles := []float32{1, 2, 4, 6}
		cumWeights := []int64{1, 2, 4, 8}

		got := IncludeMinMax(quantiles, cumWeights, 8, 1)

		wantQuantiles := []float32{1, 2, 4, 6, 8}
		wantCumWeights := []int64{1, 2, 4, 7, 8}

		assert.Equal(t, wantQuantiles, got.Quantiles)
		assert.Equal(t, wantCumWeights, got.CumWeights)
	})
}
