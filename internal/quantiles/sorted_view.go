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

// MinMaxResult holds a pair of adjusted sorted-view arrays.
type MinMaxResult[T Number] struct {
	Quantiles  []T
	CumWeights []int64
}

// IncludeMinMax reinserts the true min and max items into sorted-view
// arrays when they are missing from the retained quantiles.
//
// The returned cumulative weights remain in cumulative form. When no
// adjustment is required, the input slices are returned unchanged.
func IncludeMinMax[T Number](
	quantiles []T,
	cumWeights []int64,
	maxItem, minItem T,
) MinMaxResult[T] {
	lenIn := len(cumWeights)
	adjLow := quantiles[0] != minItem
	adjHigh := quantiles[lenIn-1] != maxItem
	adjLen := lenIn
	if adjLow {
		adjLen++
	}
	if adjHigh {
		adjLen++
	}

	if adjLen == lenIn {
		return MinMaxResult[T]{
			Quantiles:  quantiles,
			CumWeights: cumWeights,
		}
	}

	adjQuantiles := make([]T, adjLen)
	adjCumWeights := make([]int64, adjLen)
	offset := 0
	if adjLow {
		offset = 1
	}
	copy(adjQuantiles[offset:], quantiles[:lenIn])
	copy(adjCumWeights[offset:], cumWeights[:lenIn])

	if adjLow {
		adjQuantiles[0] = minItem
		adjCumWeights[0] = 1
	}

	if adjHigh {
		adjQuantiles[adjLen-1] = maxItem
		adjCumWeights[adjLen-1] = cumWeights[lenIn-1]
		adjCumWeights[adjLen-2] = cumWeights[lenIn-1] - 1
	}

	return MinMaxResult[T]{
		Quantiles:  adjQuantiles,
		CumWeights: adjCumWeights,
	}
}
