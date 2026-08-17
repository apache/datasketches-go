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

package common

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPowerSeriesNextDouble(t *testing.T) {
	tests := []struct {
		name        string
		ppb         int
		curPoint    float64
		roundToLong bool
		logBase     float64
		expected    float64
	}{
		{
			name:     "current point below one",
			ppb:      2,
			curPoint: 0.5,
			logBase:  2,
			expected: math.Sqrt2,
		},
		{
			name:     "next unrounded point",
			ppb:      2,
			curPoint: 1,
			logBase:  2,
			expected: math.Sqrt2,
		},
		{
			name:        "rounded values that do not advance are skipped",
			ppb:         4,
			curPoint:    1,
			roundToLong: true,
			logBase:     2,
			expected:    2,
		},
		{
			name:     "current point on the series",
			ppb:      2,
			curPoint: 2,
			logBase:  2,
			expected: math.Pow(2, 1.5),
		},
		{
			name:     "decimal power series",
			ppb:      1,
			curPoint: 10,
			logBase:  10,
			expected: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := PowerSeriesNextDouble(tt.ppb, tt.curPoint, tt.roundToLong, tt.logBase)
			assert.InDelta(t, tt.expected, actual, 1e-12)
		})
	}
}

func TestCeilingPowerOf2(t *testing.T) {
	const maximumPowerOfTwo = 1 << 30
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{name: "negative", input: -1, expected: 1},
		{name: "zero", input: 0, expected: 1},
		{name: "one", input: 1, expected: 1},
		{name: "power of two", input: 16, expected: 16},
		{name: "one above power of two", input: 17, expected: 32},
		{name: "one below power of two", input: 31, expected: 32},
		{name: "one below maximum", input: maximumPowerOfTwo - 1, expected: maximumPowerOfTwo},
		{name: "maximum", input: maximumPowerOfTwo, expected: maximumPowerOfTwo},
		{name: "above maximum", input: math.MaxInt, expected: maximumPowerOfTwo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, CeilingPowerOf2(tt.input))
		})
	}
}
