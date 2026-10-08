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

package tuple

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/theta"
)

func BenchmarkUpdateSketch_PointerSummary(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sketch, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		for i := 0; i < 10000; i++ {
			assert.NoError(b, sketch.UpdateInt64(int64(i), 1))
		}
	}
}

func BenchmarkUpdateSketch_ValueSummary(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sketch, _ := NewUpdateSketchWithSummaryUpdateFunc[int32ValueSummary, int32](
			newInt32ValueSummary,
			func(s int32ValueSummary, v int32) int32ValueSummary {
				s.value += v
				return s
			},
		)
		for i := 0; i < 10000; i++ {
			assert.NoError(b, sketch.UpdateInt64(int64(i), 1))
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	sketch, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
	assert.NoError(b, err)
	for i := 0; i < 10000; i++ {
		assertUpdate(b, sketch.UpdateInt64(int64(i), 1))
	}
	compact, err := sketch.Compact(true)
	assert.NoError(b, err)
	var buf bytes.Buffer
	encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
	assert.NoError(b, encoder.Encode(compact))
	data := buf.Bytes()

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := Decode(data, theta.DefaultSeed, int32SummaryReader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeArrayOfNumbers(b *testing.B) {
	source, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(b, err)
	for i := 0; i < 10000; i++ {
		assertUpdate(b, source.UpdateInt64(int64(i), []float64{1, 2}))
	}
	compact, err := source.Compact(true)
	assert.NoError(b, err)
	var buf bytes.Buffer
	encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
	assert.NoError(b, encoder.Encode(compact))
	data := buf.Bytes()

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := DecodeArrayOfNumbersCompactSketch[float64](data, theta.DefaultSeed); err != nil {
			b.Fatal(err)
		}
	}
}
