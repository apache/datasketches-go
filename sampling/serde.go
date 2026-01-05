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
	"encoding/binary"
	"errors"
	"math"
)

// ItemsSerDe defines the interface for serializing and deserializing items.
// Users must implement this interface for custom types.
// Built-in implementations are provided for common types (int64, int32, string, float64).
type ItemsSerDe[T any] interface {
	// SerializeToBytes converts items to a byte slice.
	SerializeToBytes(items []T) []byte

	// DeserializeFromBytes converts bytes back to items.
	// numItems specifies how many items to read from the data.
	DeserializeFromBytes(data []byte, numItems int) ([]T, error)

	// SizeOfItem returns the size in bytes for a single item.
	// Returns -1 for variable-length types (like string).
	SizeOfItem() int
}

// Int64SerDe provides serialization for int64 (8 bytes per item).
type Int64SerDe struct{}

func (s Int64SerDe) SerializeToBytes(items []int64) []byte {
	buf := make([]byte, len(items)*8)
	for i, v := range items {
		binary.LittleEndian.PutUint64(buf[i*8:], uint64(v))
	}
	return buf
}

func (s Int64SerDe) DeserializeFromBytes(data []byte, numItems int) ([]int64, error) {
	if len(data) < numItems*8 {
		return nil, errors.New("data too short for int64 deserialization")
	}
	items := make([]int64, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}
	return items, nil
}

func (s Int64SerDe) SizeOfItem() int {
	return 8
}

// Int32SerDe provides serialization for int32 (4 bytes per item).
type Int32SerDe struct{}

func (s Int32SerDe) SerializeToBytes(items []int32) []byte {
	buf := make([]byte, len(items)*4)
	for i, v := range items {
		binary.LittleEndian.PutUint32(buf[i*4:], uint32(v))
	}
	return buf
}

func (s Int32SerDe) DeserializeFromBytes(data []byte, numItems int) ([]int32, error) {
	if len(data) < numItems*4 {
		return nil, errors.New("data too short for int32 deserialization")
	}
	items := make([]int32, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = int32(binary.LittleEndian.Uint32(data[i*4:]))
	}
	return items, nil
}

func (s Int32SerDe) SizeOfItem() int {
	return 4
}

// Float64SerDe provides serialization for float64 (8 bytes per item).
type Float64SerDe struct{}

func (s Float64SerDe) SerializeToBytes(items []float64) []byte {
	buf := make([]byte, len(items)*8)
	for i, v := range items {
		binary.LittleEndian.PutUint64(buf[i*8:], math.Float64bits(v))
	}
	return buf
}

func (s Float64SerDe) DeserializeFromBytes(data []byte, numItems int) ([]float64, error) {
	if len(data) < numItems*8 {
		return nil, errors.New("data too short for float64 deserialization")
	}
	items := make([]float64, numItems)
	for i := 0; i < numItems; i++ {
		bits := binary.LittleEndian.Uint64(data[i*8:])
		items[i] = math.Float64frombits(bits)
	}
	return items, nil
}

func (s Float64SerDe) SizeOfItem() int {
	return 8
}

// StringSerDe provides serialization for string (variable length: 4-byte length prefix + content).
type StringSerDe struct{}

func (s StringSerDe) SerializeToBytes(items []string) []byte {
	// Calculate total size
	totalSize := 0
	for _, str := range items {
		totalSize += 4 + len(str) // 4 bytes for length + string bytes
	}

	buf := make([]byte, totalSize)
	offset := 0
	for _, str := range items {
		// Write length
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(str)))
		offset += 4
		// Write string content
		copy(buf[offset:], str)
		offset += len(str)
	}
	return buf
}

func (s StringSerDe) DeserializeFromBytes(data []byte, numItems int) ([]string, error) {
	items := make([]string, numItems)
	offset := 0
	for i := 0; i < numItems; i++ {
		if offset+4 > len(data) {
			return nil, errors.New("data too short for string length")
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		if offset+length > len(data) {
			return nil, errors.New("data too short for string content")
		}
		items[i] = string(data[offset : offset+length])
		offset += length
	}
	return items, nil
}

func (s StringSerDe) SizeOfItem() int {
	return -1 // Variable length
}
