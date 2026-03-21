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
	"errors"
	"unsafe"

	"github.com/twmb/murmur3"
)

type ItemSketchStringHasher struct{}
type ItemSketchStringSerDe struct{}

var ItemSketchStringComparator = func(reverseOrder bool) CompareFn[string] {
	return func(a, b string) bool {
		if reverseOrder {
			return a > b
		}
		return a < b
	}
}

func (f ItemSketchStringHasher) Hash(item string) uint64 {
	datum := unsafe.Slice(unsafe.StringData(item), len(item))
	return murmur3.SeedSum64(defaultSerdeHashSeed, datum[:])
}

func (f ItemSketchStringSerDe) SizeOf(item string) int {
	return len(item) + 4
}

func (f ItemSketchStringSerDe) SizeOfMany(mem []byte, offsetBytes int, numItems int) (int, error) {
	if numItems <= 0 {
		return 0, nil
	}
	reqLen := 4
	offset := offsetBytes
	memCap := len(mem)
	for i := 0; i < numItems; i++ {
		if !checkBounds(offset, reqLen, memCap) {
			return 0, errors.New("offset out of bounds")
		}
		itemLenBytes := int(binary.LittleEndian.Uint32(mem[offset:]))
		offset += 4
		if offset+itemLenBytes > memCap {
			return 0, errors.New("offset out of bounds")
		}
		offset += itemLenBytes
	}
	return offset - offsetBytes, nil
}

// SerializeOneToSlice writes the item to a byte slice.
//
// If the sketch contains string values and the caller cares about
// cross-language compatibility, it is the caller's responsibility to ensure
// that the input string is encoded as valid UTF-8.
func (f ItemSketchStringSerDe) SerializeOneToSlice(item string) []byte {
	if len(item) == 0 {
		return []byte{}
	}
	utf8len := len(item)
	bytesOut := make([]byte, utf8len+4)
	binary.LittleEndian.PutUint32(bytesOut, uint32(utf8len))
	copy(bytesOut[4:], item)
	return bytesOut
}

// SerializeManyToSlice writes items to a byte slice.
//
// If the sketch contains string values and the caller cares about
// cross-language compatibility, it is the caller's responsibility to ensure
// that those strings are encoded as valid UTF-8.
func (f ItemSketchStringSerDe) SerializeManyToSlice(items []string) []byte {
	if len(items) == 0 {
		return []byte{}
	}
	totalBytes := 0
	for _, item := range items {
		totalBytes += len(item) + 4
	}
	bytesOut := make([]byte, totalBytes)
	offset := 0
	for _, item := range items {
		utf8len := len(item)
		binary.LittleEndian.PutUint32(bytesOut[offset:], uint32(utf8len))
		offset += 4
		copy(bytesOut[offset:], item)
		offset += utf8len
	}
	return bytesOut
}

// DeserializeManyFromSlice reconstructs bytes from its serialized form.
//
// If the sketch contains string values and the caller cares about
// cross-language compatibility, it is the caller's responsibility to ensure
// that the serialized string data is encoded as valid UTF-8.
func (f ItemSketchStringSerDe) DeserializeManyFromSlice(mem []byte, offsetBytes int, numItems int) ([]string, error) {
	if numItems <= 0 {
		return []string{}, nil
	}
	array := make([]string, numItems)
	offset := offsetBytes
	intSize := 4
	memCap := len(mem)
	for i := 0; i < numItems; i++ {
		if !checkBounds(offset, intSize, memCap) {
			return nil, errors.New("offset out of bounds")
		}
		strLength := int(binary.LittleEndian.Uint32(mem[offset:]))
		offset += intSize
		utf8Bytes := make([]byte, strLength)
		if !checkBounds(offset, strLength, memCap) {
			return nil, errors.New("offset out of bounds")
		}
		copy(utf8Bytes, mem[offset:offset+strLength])
		offset += strLength
		array[i] = string(utf8Bytes)
	}
	return array, nil
}
