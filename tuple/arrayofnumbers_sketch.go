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

// Number constraint for types that can be used in ArrayOfNumbersSummary.
// NOTE: Only allowed fixed-size type.
type Number interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

type ArrayOfNumbersSketch[V Number] interface {
	Sketch[*ArrayOfNumbersSummary[V]]

	// NumValuesInSummary returns the number of values in ArrayOfNumbersSummary.
	NumValuesInSummary() uint8
}

// ArrayOfNumbersSummary is a fixed-size array of numeric values used as a summary in tuple sketches.
type ArrayOfNumbersSummary[T Number] struct {
	values []T
	size   uint8
}

func newArrayOfNumbersSummary[T Number](size uint8) *ArrayOfNumbersSummary[T] {
	return &ArrayOfNumbersSummary[T]{
		values: make([]T, size),
		size:   size,
	}
}

func newArrayOfNumbersSummaryFromValues[T Number](values []T, size uint8) *ArrayOfNumbersSummary[T] {
	return &ArrayOfNumbersSummary[T]{
		values: values,
		size:   size,
	}
}

// Reset clears the content of the summary, restoring it to its initial state.
func (s *ArrayOfNumbersSummary[T]) Reset() {
	s.values = make([]T, s.size)
}

// Clone creates and returns a deep copy of the current Summary instance.
func (s *ArrayOfNumbersSummary[T]) Clone() Summary {
	return &ArrayOfNumbersSummary[T]{
		values: append([]T{}, s.values...),
		size:   s.size,
	}
}

// Update incorporates a new value into the summary, modifying its internal state based on the given input value.
func (s *ArrayOfNumbersSummary[T]) Update(values []T) {
	for i := 0; i < int(s.size); i++ {
		s.values[i] += values[i]
	}
}

// Values returns the values in the summary.
func (s *ArrayOfNumbersSummary[T]) Values() []T {
	return s.values
}
