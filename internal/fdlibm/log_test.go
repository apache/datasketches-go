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

package fdlibm

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogMatchesJavaStrictMath(t *testing.T) {
	for _, tc := range []struct {
		x    float64
		bits uint64
	}{
		{1, 0x0000000000000000},
		{2, 0x3fe62e42fefa39ef},
		{1024, 0x401bb9d3beb8c86b},
		{1 + 0x1p-30, 0x3e0fffffffc00000},
		{8 * (1 + 0x1p-30), 0x4000a2b23f5bab73},
		{1.1, 0x3fb8663f793c46cc},
		{1.41, 0x3fd5fd5fabe64084},
		{1.41 * 32, 0x400e797fb435907c},
		{0.75, 0xbfd269621134db92},
		{1e-300, 0xc085963447f87fb5},
		{5e-320, 0xc086f9be0f7cee66},
		{1e300, 0x4085963447f87fb5},
		{math.MaxFloat64, 0x40862e42fefa39ef},
		{math.SmallestNonzeroFloat64, 0xc0874385446d71c3},
		{176, 0x4014ae935b3cb89d},
		{48, 0x400ef8383c50bb74},
		{74, 0x4011375cd6fcab1c},
		{185, 0x4014e1a4f518c72c},
		{196, 0x40151cca16d7bba8},
		{299, 0x4016cd411481a020},
		{308, 0x4016eb9f470ac0b8},
		{334, 0x40173e9bbe951e9c},
		{343, 0x401759d602a5c3c2},
		{1261, 0x401c8f031e7e1220},
	} {
		assert.Equal(t, tc.bits, math.Float64bits(Log(tc.x)), "log(%v)", tc.x)
	}
}

func TestLogSpecialValues(t *testing.T) {
	assert.Equal(t, math.Inf(-1), Log(0))
	assert.Equal(t, math.Inf(-1), Log(math.Copysign(0, -1)))
	assert.Equal(t, math.Inf(1), Log(math.Inf(1)))
	assert.True(t, math.IsNaN(Log(-1)))
	assert.True(t, math.IsNaN(Log(math.Inf(-1))))
	assert.True(t, math.IsNaN(Log(math.NaN())))
}

func TestLogWithinOneULPOfMathLog(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 100000 {
		x := math.Float64frombits(r.Uint64() &^ (1 << 63))
		// math.Log on amd64 is wrong for subnormals; the Java table covers them
		if x < 0x1p-1022 || math.IsInf(x, 0) || math.IsNaN(x) || x == 1 {
			continue
		}
		got, want := math.Float64bits(Log(x)), math.Float64bits(math.Log(x))
		assert.LessOrEqual(t, max(got, want)-min(got, want), uint64(1), "log(%v)", x)
	}
}
