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

package common

import (
	"encoding/binary"
	"math"
)

var ItemSketchFloatComparator = func(reverseOrder bool) CompareFn[float32] {
	return func(a float32, b float32) bool {
		if reverseOrder {
			return a > b
		}
		return a < b
	}
}

// ItemSketchFloatSerDe handles serialization and deserialization of floating-point sketch items.
type ItemSketchFloatSerDe struct{}

func (s ItemSketchFloatSerDe) SizeOf(item float32) int {
	return 4
}

func (s ItemSketchFloatSerDe) SizeOfMany(mem []byte, offsetBytes int, numItems int) (int, error) {
	return numItems * 4, nil
}

func (s ItemSketchFloatSerDe) SerializeOneToSlice(item float32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, math.Float32bits(item))
	return bytes
}

func (s ItemSketchFloatSerDe) SerializeManyToSlice(items []float32) []byte {
	if len(items) == 0 {
		return []byte{}
	}

	bytes := make([]byte, 4*len(items))
	offset := 0
	for _, item := range items {
		binary.LittleEndian.PutUint32(bytes[offset:], math.Float32bits(item))
		offset += 4
	}
	return bytes
}

func (s ItemSketchFloatSerDe) DeserializeManyFromSlice(mem []byte, offsetBytes int, numItems int) ([]float32, error) {
	if numItems == 0 {
		return []float32{}, nil
	}

	array := make([]float32, 0, numItems)
	for i := 0; i < numItems; i++ {
		array = append(array, math.Float32frombits(binary.LittleEndian.Uint32(mem[offsetBytes:])))
		offsetBytes += 4
	}
	return array, nil
}
