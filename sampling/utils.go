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

package sampling

import (
	"math"

	"github.com/apache/datasketches-go/internal/binomialproportionsbounds"
)

const (
	// defaultKappa is Number of standard deviations to use for subset sum error bounds
	defaultKappa = 2.0
)

func startingSubMultiple(lgTarget, lgRf, lgMin int) int {
	if lgTarget <= lgMin {
		return lgMin
	}
	if lgRf == 0 {
		return lgTarget
	}
	return (lgTarget-lgMin)%lgRf + lgMin
}

// adjustedSamplingAllocationSize checks target sampling allocation is more than
// 50% of max sampling size. If so, return max sampling size, otherwise passes
// through the target size.
func adjustedSamplingAllocationSize(
	maxSize, resizeTarget int,
) int {
	if maxSize-(resizeTarget<<1) < 0 {
		return maxSize
	}
	return resizeTarget
}

// SampleSubsetSummary is a simple object that captures the results of a subset sum query on a sampling sketch.
type SampleSubsetSummary struct {
	LowerBound        float64
	Estimate          float64
	UpperBound        float64
	TotalSketchWeight float64
}

func pseudoHypergeometricUpperBoundOnP(n, k uint64, samplingRate float64) (float64, error) {
	return binomialproportionsbounds.ApproximateUpperBoundOnP(n, k, defaultKappa*math.Sqrt(1-samplingRate))
}

func pseudoHypergeometricLowerBoundOnP(n, k uint64, samplingRate float64) (float64, error) {
	return binomialproportionsbounds.ApproximateLowerBoundOnP(n, k, defaultKappa*math.Sqrt(1-samplingRate))
}
