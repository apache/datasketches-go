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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateSplitPoints(t *testing.T) {
	tests := []struct {
		name    string
		values  []float64
		wantErr error
	}{
		{"empty slice", []float64{}, nil},
		{"single element", []float64{1.0}, nil},
		{"valid increasing", []float64{1.0, 2.0, 3.0}, nil},
		{"NaN", []float64{math.NaN(), 2.0}, ErrNanInSplitPoints},
		{"duplicate values", []float64{1.0, 1.0, 2.0}, ErrInvalidSplitPoints},
		{"decreasing", []float64{3.0, 2.0, 1.0}, ErrInvalidSplitPoints},
		{"not strictly increasing at end", []float64{1.0, 2.0, 2.0}, ErrInvalidSplitPoints},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSplitPoints(tt.values)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateNormalizedRankBounds(t *testing.T) {
	tests := []struct {
		name    string
		rank    float64
		wantErr string
	}{
		{"below zero", -0.1, "rank must be between 0 and 1 inclusive"},
		{"zero", 0, ""},
		{"middle", 0.5, ""},
		{"one", 1, ""},
		{"above one", 1.1, "rank must be between 0 and 1 inclusive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNormalizedRankBounds(tt.rank)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestComputeNaturalRank(t *testing.T) {
	rankJustBelowThreeNoRounding := math.Nextafter(3.0/10000001.0, 0)

	tests := []struct {
		name           string
		normalizedRank float64
		totalN         uint64
		inclusive      bool
		want           int64
	}{
		{"zero exclusive", 0, 10, false, 0},
		{"zero inclusive", 0, 10, true, 0},
		{"one exclusive", 1, 10, false, 10},
		{"one inclusive", 1, 10, true, 10},
		{"exact integer exclusive", 0.5, 10, false, 5},
		{"exact integer inclusive", 0.5, 10, true, 5},
		{"fractional exclusive floors", 0.21, 10, false, 2},
		{"fractional inclusive ceils", 0.21, 10, true, 3},
		{"rounding enabled exclusive", 0.299999996, 10, false, 3},
		{"rounding enabled inclusive", 0.299999996, 10, true, 3},
		{"rounding disabled exclusive", rankJustBelowThreeNoRounding, 10000001, false, 2},
		{"rounding disabled inclusive", rankJustBelowThreeNoRounding, 10000001, true, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeNaturalRank(tt.normalizedRank, tt.totalN, tt.inclusive)
			assert.Equal(t, tt.want, got)
		})
	}
}
