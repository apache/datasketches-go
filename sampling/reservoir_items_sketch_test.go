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

func TestNewReservoirItemsSketch(t *testing.T) {
	sketch, err := NewReservoirItemsSketch[int64](10)
	assert.NoError(t, err)
	assert.NotNil(t, sketch)
	assert.Equal(t, 10, sketch.K())
	assert.Equal(t, int64(0), sketch.N())
	assert.True(t, sketch.IsEmpty())
}

func TestReservoirItemsSketchWithStrings(t *testing.T) {
	sketch, err := NewReservoirItemsSketch[string](5)
	assert.NoError(t, err)

	sketch.Update("apple")
	sketch.Update("banana")
	sketch.Update("cherry")

	assert.Equal(t, int64(3), sketch.N())
	assert.Equal(t, 3, sketch.NumSamples())

	samples := sketch.Samples()
	assert.Contains(t, samples, "apple")
	assert.Contains(t, samples, "banana")
	assert.Contains(t, samples, "cherry")
}

func TestReservoirItemsSketchWithStruct(t *testing.T) {
	type Event struct {
		ID   int
		Name string
	}

	sketch, err := NewReservoirItemsSketch[Event](5)
	assert.NoError(t, err)

	sketch.Update(Event{1, "login"})
	sketch.Update(Event{2, "logout"})
	sketch.Update(Event{3, "click"})

	assert.Equal(t, int64(3), sketch.N())
	samples := sketch.Samples()
	assert.Len(t, samples, 3)
}

func TestReservoirItemsSketchInvalidK(t *testing.T) {
	_, err := NewReservoirItemsSketch[int64](0)
	assert.Error(t, err)

	_, err = NewReservoirItemsSketch[int64](-1)
	assert.Error(t, err)
}

func TestReservoirItemsSketchUpdateBelowK(t *testing.T) {
	sketch, _ := NewReservoirItemsSketch[int64](10)

	for i := int64(1); i <= 5; i++ {
		sketch.Update(i)
	}

	assert.Equal(t, int64(5), sketch.N())
	assert.Equal(t, 5, sketch.NumSamples())

	samples := sketch.Samples()
	for i := int64(1); i <= 5; i++ {
		assert.Contains(t, samples, i)
	}
}

func TestReservoirItemsSketchUpdateAboveK(t *testing.T) {
	sketch, _ := NewReservoirItemsSketch[int64](10)

	for i := int64(1); i <= 1000; i++ {
		sketch.Update(i)
	}

	assert.Equal(t, int64(1000), sketch.N())
	assert.Equal(t, 10, sketch.NumSamples())
}

func TestReservoirItemsSketchReset(t *testing.T) {
	sketch, _ := NewReservoirItemsSketch[int64](10)

	for i := int64(1); i <= 5; i++ {
		sketch.Update(i)
	}

	sketch.Reset()
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, int64(0), sketch.N())
	assert.Equal(t, 10, sketch.K())
}

func TestReservoirItemsSketchKEqualsOne(t *testing.T) {
	// Edge case: k=1 should keep exactly one sample
	sketch, err := NewReservoirItemsSketch[int64](1)
	assert.NoError(t, err)

	for i := int64(1); i <= 100; i++ {
		sketch.Update(i)
	}

	assert.Equal(t, 1, sketch.NumSamples())
	assert.Equal(t, int64(100), sketch.N())
}

func TestReservoirItemsSketchGetSamplesIsCopy(t *testing.T) {
	sketch, _ := NewReservoirItemsSketch[int64](10)
	sketch.Update(42)

	samples1 := sketch.Samples()
	samples2 := sketch.Samples()

	// Modify samples1
	samples1[0] = 999

	// samples2 and internal data should be unchanged
	assert.NotEqual(t, samples1[0], samples2[0])
	assert.Equal(t, int64(42), samples2[0])
}
