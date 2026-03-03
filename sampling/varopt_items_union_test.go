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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewVarOptItemsUnion(t *testing.T) {
	_, err := NewVarOptItemsUnion[int](0)
	assert.ErrorContains(t, err, "k must be at least 1")

	union, err := NewVarOptItemsUnion[int](16)
	assert.NoError(t, err)
	assert.Equal(t, 16, union.MaxK())
}

func TestVarOptItemsUnion_ResultEmpty(t *testing.T) {
	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, 8, result.K())
	assert.Equal(t, int64(0), result.N())
	assert.True(t, result.IsEmpty())
}

func TestVarOptItemsUnion_UpdateSketchExactMode(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int](16)
	assert.NoError(t, err)

	for i := 0; i < 8; i++ {
		assert.NoError(t, sketch.Update(i, float64(i+1)))
	}

	union, err := NewVarOptItemsUnion[int](16)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(sketch))

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(8), result.N())
	assert.Equal(t, 8, result.NumSamples())
	assert.Equal(t, 8, result.H())
	assert.Equal(t, 0, result.R())
}

func TestVarOptItemsUnion_UpdateSketchNilNoop(t *testing.T) {
	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)

	assert.NoError(t, union.UpdateSketch(nil))

	result, err := union.Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), result.N())
}

func TestVarOptItemsUnion_Reset(t *testing.T) {
	sketch, err := NewVarOptItemsSketch[int](8)
	assert.NoError(t, err)
	for i := 0; i < 4; i++ {
		assert.NoError(t, sketch.Update(i, float64(i+1)))
	}

	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(sketch))

	assert.NoError(t, union.Reset())

	result, err := union.Result()
	assert.NoError(t, err)
	assert.True(t, result.IsEmpty())
	assert.Equal(t, 8, result.K())
}

func TestVarOptItemsUnion_ResultPseudoExactMarkedResolution(t *testing.T) {
	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)

	// Construct a pseudo-exact gadget: r=0 with marked items in H.
	for i := 1; i <= 4; i++ {
		assert.NoError(t, union.gadget.update(i, float64(i), true))
	}
	union.n = 4
	union.outerTauDenom = int64(union.gadget.numMarksInH)

	out, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, int64(4), out.N())
	assert.Equal(t, 4, out.K())
	assert.Equal(t, 0, out.H())
	assert.Equal(t, 4, out.R())
	assert.Nil(t, out.marks)
	assert.Equal(t, uint32(0), out.numMarksInH)
}

func TestVarOptItemsUnion_ResultMigrateMarkedItemsByDecreasingK(t *testing.T) {
	union, err := NewVarOptItemsUnion[int](8)
	assert.NoError(t, err)

	// Construct a compact, valid estimation-mode gadget with one marked item in H.
	// Layout: [H=0] [gap=1] [R=2], with k=2, h=1, r=1.
	union.gadget = &VarOptItemsSketch[int]{
		data:         []int{100, 0, 1},
		weights:      []float64{10.0, -1.0, -1.0},
		marks:        []bool{true, false, false},
		k:            2,
		n:            10,
		h:            1,
		m:            0,
		r:            1,
		totalWeightR: 5.0,
		rf:           varOptDefaultResizeFactor,
		numMarksInH:  1,
	}
	union.n = 10

	out, err := union.Result()
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, int64(10), out.N())
	assert.Nil(t, out.marks)
	assert.Equal(t, uint32(0), out.numMarksInH)
}
