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

import "testing"

func BenchmarkUpdateSketch_PointerSummary(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sketch, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		for i := 0; i < 10000; i++ {
			sketch.UpdateInt64(int64(i), 1)
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
			sketch.UpdateInt64(int64(i), 1)
		}
	}
}
