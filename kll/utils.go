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
	"errors"
	"math"
)

const (
	tailRoundingFactor = 1e7
)

func convertToCumulative(array []int64) int64 {
	subtotal := int64(0)
	for i := range array {
		subtotal += array[i]
		array[i] = subtotal
	}
	return subtotal
}

func getNaturalRank(normalizedRank float64, totalN uint64, inclusive bool) int64 {
	naturalRank := normalizedRank * float64(totalN)
	if totalN <= tailRoundingFactor {
		naturalRank = math.Round(naturalRank*tailRoundingFactor) / tailRoundingFactor
	}
	if inclusive {
		return int64(math.Ceil(naturalRank))
	}
	return int64(math.Floor(naturalRank))
}

func checkNormalizedRankBounds(rank float64) error {
	if rank < 0 || rank > 1 {
		return errors.New("rank must be between 0 and 1 inclusive")
	}
	return nil
}
