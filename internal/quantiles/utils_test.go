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
