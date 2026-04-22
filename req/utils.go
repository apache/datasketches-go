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

// ComputeRSE returns an a priori estimate of relative standard error (ComputeRSE, expressed as a number in [0,1]).
// Derived from Lemma 12 in https://arxiv.org/abs/2004.01668v2, but the constant factors were
// adjusted based on empirical measurements.
// K is the sketch's k.
// Rank is the normalized rank, in [0,1].
// isHighRankAccuracyMode is true if the sketch is configured for high rank accuracy.
// N is the total number of items in the stream.
func ComputeRSE(k int, rank float64, isHighRankAccuracyMode bool, n int64) float64 {
	return computeRankUpperBound(k, 2, rank, 1, isHighRankAccuracyMode, n) - rank
}
