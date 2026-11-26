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

package hll

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHarmonicNumberMatchesJava(t *testing.T) {
	// expected bits are datasketches-java's formulas with StrictMath.log; a 1 ULP error in log(185)
	// becomes 16 ULPs in the bitmap estimate, which subtracts two nearby harmonic numbers
	assert.Equal(t, uint64(0x4017337a708f09c3), math.Float64bits(harmonicNumber(185)))
	assert.Equal(t, uint64(0x4054bd9d8a3da230), math.Float64bits(getHllBitMapEstimate(8, 0, 185)))
}
