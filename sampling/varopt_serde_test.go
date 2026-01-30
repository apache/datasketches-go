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
	"bytes"
	"math"
	"testing"
)

func TestVarOptItemsSketch_RoundTripWarmup(t *testing.T) {
	sketch, _ := NewVarOptItemsSketch[int64](16)
	for i := int64(1); i <= 5; i++ {
		if err := sketch.Update(i, float64(i)); err != nil {
			t.Fatalf("update failed: %v", err)
		}
	}

	buf := &bytes.Buffer{}
	enc := NewVarOptItemsSketchEncoder[int64](buf, Int64SerDe{})
	if err := enc.Encode(sketch); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	dec := NewVarOptItemsSketchDecoder[int64](bytes.NewReader(buf.Bytes()), Int64SerDe{})
	restored, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if restored.K() != sketch.K() || restored.N() != sketch.N() {
		t.Fatalf("metadata mismatch: k=%d/%d n=%d/%d", restored.K(), sketch.K(), restored.N(), sketch.N())
	}
	if restored.H() != sketch.H() || restored.R() != sketch.R() {
		t.Fatalf("region mismatch: h=%d/%d r=%d/%d", restored.H(), sketch.H(), restored.R(), sketch.R())
	}

	sum := 0.0
	for sample := range restored.All() {
		sum += sample.Weight
	}
	if math.Abs(sum-15.0) > 1e-12 {
		t.Fatalf("unexpected total weight: %f", sum)
	}

	buf2 := &bytes.Buffer{}
	enc2 := NewVarOptItemsSketchEncoder[int64](buf2, Int64SerDe{})
	if err := enc2.Encode(restored); err != nil {
		t.Fatalf("encode after restore failed: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), buf2.Bytes()) {
		t.Fatalf("round-trip bytes mismatch")
	}
}

func TestVarOptItemsSketch_RoundTripSampling(t *testing.T) {
	sketch, _ := NewVarOptItemsSketch[int64](32)
	for i := int64(0); i < 100; i++ {
		if err := sketch.Update(i, 1.0); err != nil {
			t.Fatalf("update failed: %v", err)
		}
	}

	buf := &bytes.Buffer{}
	enc := NewVarOptItemsSketchEncoder[int64](buf, Int64SerDe{})
	if err := enc.Encode(sketch); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	dec := NewVarOptItemsSketchDecoder[int64](bytes.NewReader(buf.Bytes()), Int64SerDe{})
	restored, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if restored.K() != sketch.K() || restored.N() != sketch.N() {
		t.Fatalf("metadata mismatch: k=%d/%d n=%d/%d", restored.K(), sketch.K(), restored.N(), sketch.N())
	}
	if restored.NumSamples() != sketch.NumSamples() {
		t.Fatalf("sample count mismatch: %d/%d", restored.NumSamples(), sketch.NumSamples())
	}

	sum := 0.0
	for sample := range restored.All() {
		sum += sample.Weight
	}
	if math.Abs(sum-float64(sketch.N())) > 1e-9 {
		t.Fatalf("unexpected total weight: %f", sum)
	}

	buf2 := &bytes.Buffer{}
	enc2 := NewVarOptItemsSketchEncoder[int64](buf2, Int64SerDe{})
	if err := enc2.Encode(restored); err != nil {
		t.Fatalf("encode after restore failed: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), buf2.Bytes()) {
		t.Fatalf("round-trip bytes mismatch")
	}
}

func TestVarOptItemsSketch_GadgetMarksRoundTrip(t *testing.T) {
	sketch, _ := NewVarOptItemsSketch[int64](16)
	for i := int64(0); i < 50; i++ {
		if err := sketch.Update(i, 1.0); err != nil {
			t.Fatalf("update failed: %v", err)
		}
	}

	sketch.marks = make([]bool, sketch.allocatedSize)
	for i := 0; i < sketch.h; i++ {
		if (i & 0x1) == 0 {
			sketch.marks[i] = true
		}
	}

	buf := &bytes.Buffer{}
	enc := NewVarOptItemsSketchEncoder[int64](buf, Int64SerDe{})
	if err := enc.Encode(sketch); err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	data := buf.Bytes()
	if (data[3] & varOptFlagGadget) == 0 {
		t.Fatalf("expected gadget flag to be set")
	}

	dec := NewVarOptItemsSketchDecoder[int64](bytes.NewReader(data), Int64SerDe{})
	restored, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if restored.marks == nil {
		t.Fatalf("expected marks to be allocated")
	}
	for i := 0; i < restored.h; i++ {
		expected := (i & 0x1) == 0
		if restored.marks[i] != expected {
			t.Fatalf("mark mismatch at %d: got %v want %v", i, restored.marks[i], expected)
		}
	}

	buf2 := &bytes.Buffer{}
	enc2 := NewVarOptItemsSketchEncoder[int64](buf2, Int64SerDe{})
	if err := enc2.Encode(restored); err != nil {
		t.Fatalf("encode after restore failed: %v", err)
	}
	if !bytes.Equal(data, buf2.Bytes()) {
		t.Fatalf("round-trip bytes mismatch")
	}
}
