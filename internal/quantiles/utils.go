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
	"errors"
	"math"
)

const tailRoundingFactor = 1e7

// errors.
var (
	ErrNanInSplitPoints   = errors.New("NaN in split points")
	ErrInvalidSplitPoints = errors.New("values must be unique and monotonically increasing")
)

type Number interface {
	float32 | float64 | int64
}

func ValidateSplitPoints[N Number](values []N) error {
	for i, v := range values {
		if math.IsNaN(float64(v)) {
			return ErrNanInSplitPoints
		}
		if i < len(values)-1 && !(v < values[i+1]) {
			return ErrInvalidSplitPoints
		}
	}
	return nil
}

func ValidateNormalizedRankBounds(rank float64) error {
	if rank < 0 || rank > 1 {
		return errors.New("rank must be between 0 and 1 inclusive")
	}
	return nil
}

// ComputeNaturalRank Computes the closest Natural Rank from a given Normalized Rank
func ComputeNaturalRank(normalizedRank float64, totalN uint64, inclusive bool) int64 {
	naturalRank := normalizedRank * float64(totalN)
	if totalN <= tailRoundingFactor {
		naturalRank = math.Round(naturalRank*tailRoundingFactor) / tailRoundingFactor
	}
	if inclusive {
		return int64(math.Ceil(naturalRank))
	}
	return int64(math.Floor(naturalRank))
}
