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
	"math"
	"math/rand"
	"testing"
)

func TestVarOptItemsSketch_NewSketch(t *testing.T) {
	// Test valid k
	sketch, err := NewVarOptItemsSketch[string](16)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sketch.K() != 16 {
		t.Errorf("expected K=16, got %d", sketch.K())
	}
	if sketch.N() != 0 {
		t.Errorf("expected N=0, got %d", sketch.N())
	}
	if !sketch.IsEmpty() {
		t.Error("expected empty sketch")
	}

	// Test k too small
	_, err = NewVarOptItemsSketch[string](4)
	if err == nil {
		t.Error("expected error for k < 8")
	}

	// Test k too large
	_, err = NewVarOptItemsSketch[string](varOptMaxK + 1)
	if err == nil {
		t.Error("expected error for k > varOptMaxK")
	}

	_, err = NewVarOptItemsSketch[string](16, WithResizeFactor(ResizeFactor(3)))
	if err == nil {
		t.Error("expected error for unsupported resize factor")
	}
}

func TestVarOptItemsSketch_WarmupPhase(t *testing.T) {
	sketch, _ := NewVarOptItemsSketch[int](10)

	// Add fewer than k items - should all be stored
	for i := 1; i <= 5; i++ {
		err := sketch.Update(i, float64(i))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if sketch.N() != 5 {
		t.Errorf("expected N=5, got %d", sketch.N())
	}
	if sketch.NumSamples() != 5 {
		t.Errorf("expected NumSamples=5, got %d", sketch.NumSamples())
	}
	if !sketch.inWarmup() {
		t.Error("expected to still be in warmup mode")
	}
}

func TestVarOptItemsSketch_TransitionToEstimation(t *testing.T) {
	sketch, _ := NewVarOptItemsSketch[int](8)

	// Need k+1 items to trigger transition (h > k condition)
	for i := 1; i <= 9; i++ {
		err := sketch.Update(i, float64(i))
		if err != nil {
			t.Fatalf("unexpected error at i=%d: %v", i, err)
		}
	}

	if sketch.N() != 9 {
		t.Errorf("expected N=9, got %d", sketch.N())
	}
	// After transition, H + R should equal k
	if sketch.NumSamples() != 8 {
		t.Errorf("expected NumSamples=8, got %d", sketch.NumSamples())
	}
	// Should have transitioned out of warmup
	if sketch.inWarmup() {
		t.Error("expected to NOT be in warmup mode after filling")
	}
	// Should have some items in R region
	if sketch.R() == 0 {
		t.Error("expected R > 0 after transition")
	}
}

func TestVarOptItemsSketch_EstimationMode(t *testing.T) {
	sketch, _ := NewVarOptItemsSketch[int](8)

	// Fill and then add more
	for i := 1; i <= 20; i++ {
		err := sketch.Update(i, float64(i))
		if err != nil {
			t.Fatalf("unexpected error at i=%d: %v", i, err)
		}
	}

	if sketch.N() != 20 {
		t.Errorf("expected N=20, got %d", sketch.N())
	}
	// Should still have at most k samples
	if sketch.NumSamples() > sketch.K() {
		t.Errorf("expected NumSamples <= K, got %d > %d", sketch.NumSamples(), sketch.K())
	}
	// H + R should be <= k
	if sketch.H()+sketch.R() > sketch.K() {
		t.Errorf("expected H+R <= K, got %d+%d > %d", sketch.H(), sketch.R(), sketch.K())
	}
}

func TestVarOptItemsSketch_InvalidWeight(t *testing.T) {
	sketch, _ := NewVarOptItemsSketch[string](8)

	// Negative weight should error
	err := sketch.Update("a", -1.0)
	if err == nil {
		t.Error("expected error for negative weight")
	}

	// Zero weight is valid in C++/Java - just ignored
	err = sketch.Update("b", 0.0)
	if err != nil {
		t.Errorf("zero weight should be valid (ignored), got error: %v", err)
	}
	// Sketch should still be empty since zero weight is ignored
	if sketch.N() != 0 {
		t.Errorf("expected N=0 after zero weight update, got %d", sketch.N())
	}
}

func TestVarOptItemsSketch_Reset(t *testing.T) {
	sketch, _ := NewVarOptItemsSketch[int](8)

	for i := 1; i <= 10; i++ {
		sketch.Update(i, float64(i))
	}

	sketch.Reset()

	if !sketch.IsEmpty() {
		t.Error("expected empty after reset")
	}
	if sketch.N() != 0 {
		t.Errorf("expected N=0 after reset, got %d", sketch.N())
	}
	if sketch.H() != 0 || sketch.R() != 0 {
		t.Errorf("expected H=0, R=0 after reset, got H=%d, R=%d", sketch.H(), sketch.R())
	}
}

func TestVarOptItemsSketch_UniformWeights(t *testing.T) {
	// With uniform weights, VarOpt should behave like reservoir sampling
	sketch, _ := NewVarOptItemsSketch[int](10)

	for i := 1; i <= 100; i++ {
		sketch.Update(i, 1.0)
	}

	if sketch.N() != 100 {
		t.Errorf("expected N=100, got %d", sketch.N())
	}
	if sketch.NumSamples() > sketch.K() {
		t.Errorf("expected NumSamples <= K, got %d", sketch.NumSamples())
	}
}

func TestVarOptItemsSketch_CumulativeWeight(t *testing.T) {
	// This test verifies that the sum of output weights equals the sum of input weights.
	// This is a key property of VarOpt sketches.
	// Matches C++ test: "varopt sketch: cumulative weight"
	const eps = 1e-13
	k := 256
	n := 10 * k

	sketch, _ := NewVarOptItemsSketch[int](k)

	inputSum := 0.0
	for i := 0; i < n; i++ {
		// Generate weights using exp(5*N(0,1)) to cover ~10 orders of magnitude
		// This matches the C++ test distribution
		w := math.Exp(5 * randNormal())
		inputSum += w
		sketch.Update(i, w)
	}

	// Get output weights using Go 1.23 iterator
	outputSum := 0.0
	for sample := range sketch.All() {
		outputSum += sample.Weight
	}

	// The ratio should be exactly 1.0 (within floating point precision)
	ratio := outputSum / inputSum
	if math.Abs(ratio-1.0) > eps {
		t.Errorf("weight ratio out of expected range: got %f, expected 1.0 (±%e)", ratio, eps)
	}
}

// randNormal returns a random number from standard normal distribution N(0,1)
func randNormal() float64 {
	// Box-Muller transform
	u1 := rand.Float64()
	u2 := rand.Float64()
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}
