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

package examples

import (
	"fmt"
	"testing"

	"github.com/apache/datasketches-go/tuple"
	"github.com/stretchr/testify/assert"
)

// SumSummary is a custom Summary type that sums float64 values.
type SumSummary struct {
	sum   float64
	count int
}

func (s *SumSummary) Reset() {
	s.sum = 0
	s.count = 0
}

func (s *SumSummary) Clone() tuple.Summary {
	return &SumSummary{sum: s.sum, count: s.count}
}

func (s *SumSummary) Update(value float64) {
	s.sum += value
	s.count++
}

func (s *SumSummary) GetSum() float64 { return s.sum }
func (s *SumSummary) GetCount() int   { return s.count }
func (s *SumSummary) String() string  { return fmt.Sprintf("{sum: %.2f, count: %d}", s.sum, s.count) }
func newSumSummary() *SumSummary      { return &SumSummary{} }

// SumMergePolicy implements tuple.Policy for merging SumSummary instances.
type SumMergePolicy struct{}

func (p *SumMergePolicy) Apply(internal *SumSummary, incoming *SumSummary) {
	internal.sum += incoming.sum
	internal.count += incoming.count
}

func TestTupleSketch(t *testing.T) {
	// Create a Tuple Sketch with custom Summary
	sketch, err := tuple.NewUpdateSketch[*SumSummary, float64](newSumSummary)
	assert.NoError(t, err)

	// Update with aggregated data (customer spending)
	_ = sketch.UpdateString("alice", 100.50)
	_ = sketch.UpdateString("alice", 50.25)
	_ = sketch.UpdateString("alice", 75.00)
	_ = sketch.UpdateString("bob", 200.00)
	_ = sketch.UpdateString("bob", 30.00)

	for i := 0; i < 100; i++ {
		_ = sketch.UpdateString(fmt.Sprintf("customer_%d", i), 10.0)
	}

	// Verify distinct count
	assert.InDelta(t, 102, sketch.Estimate(), 10)

	// Verify aggregated values
	for _, summary := range sketch.All() {
		if summary.GetCount() == 3 {
			assert.InDelta(t, 225.75, summary.GetSum(), 0.01)
		}
		if summary.GetCount() == 2 && summary.GetSum() > 200 {
			assert.InDelta(t, 230.00, summary.GetSum(), 0.01)
		}
	}

	// Create a second sketch for set operations
	sketch2, err := tuple.NewUpdateSketch[*SumSummary, float64](newSumSummary)
	assert.NoError(t, err)
	_ = sketch2.UpdateString("alice", 150.00)
	_ = sketch2.UpdateString("diana", 300.00)

	// Compact the sketches
	compact1, err := sketch.Compact(true)
	assert.NoError(t, err)
	compact2, err := sketch2.Compact(true)
	assert.NoError(t, err)

	// Union with custom merge policy
	mergePolicy := &SumMergePolicy{}
	union, err := tuple.NewUnion[*SumSummary](mergePolicy)
	assert.NoError(t, err)
	_ = union.Update(compact1)
	_ = union.Update(compact2)

	unionResult, err := union.Result(true)
	assert.NoError(t, err)
	assert.InDelta(t, 103, unionResult.Estimate(), 10)

	// Intersection
	intersection := tuple.NewIntersection[*SumSummary](mergePolicy)
	_ = intersection.Update(compact1)
	_ = intersection.Update(compact2)

	intersectionResult, err := intersection.Result(true)
	assert.NoError(t, err)
	assert.InDelta(t, 1, intersectionResult.Estimate(), 1)
}
