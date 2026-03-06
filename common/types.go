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

type CompareFn[C comparable] func(C, C) bool

type ItemSketchHasher[C comparable] interface {
	Hash(item C) uint64
}

type ItemSketchSerde[C comparable] interface {
	SizeOf(item C) int
	SizeOfMany(mem []byte, offsetBytes int, numItems int) (int, error)
	SerializeManyToSlice(items []C) []byte
	SerializeOneToSlice(item C) []byte
	DeserializeManyFromSlice(mem []byte, offsetBytes int, numItems int) ([]C, error)
}

// ItemSketchSerdeWithValidation extends ItemSketchSerde by adding validation for single and multiple items.
type ItemSketchSerdeWithValidation[C comparable] interface {
	ItemSketchSerde[C]

	// ValidateOne checks the validity of a single item of type C and returns an error if it does not meet the criteria.
	// ValidateOne executed before serialization or after deserialization.
	ValidateOne(item C) error

	// ValidateMany checks the validity of multiple items of type C and returns an error if any item does not meet the criteria.
	// ValidateMany executed before serialization or after deserialization.
	ValidateMany(items []C) error
}
