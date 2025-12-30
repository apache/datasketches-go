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

package filters

import "math/bits"

// getBit returns the value of the bit at the specified index.
func getBit(array []uint64, index uint64) bool {
	longIdx := index >> 6  // divide by 64
	bitIdx := index & 0x3F // mod 64
	return (array[longIdx] & (1 << bitIdx)) != 0
}

// setBit sets the bit at the specified index to 1.
func setBit(array []uint64, index uint64) {
	longIdx := index >> 6
	bitIdx := index & 0x3F
	array[longIdx] |= (1 << bitIdx)
}

// clearBit sets the bit at the specified index to 0.
func clearBit(array []uint64, index uint64) {
	longIdx := index >> 6
	bitIdx := index & 0x3F
	array[longIdx] &^= (1 << bitIdx)
}

// assignBit sets the bit at the specified index to the given value.
func assignBit(array []uint64, index uint64, value bool) {
	if value {
		setBit(array, index)
	} else {
		clearBit(array, index)
	}
}

// getAndSetBit gets the current bit value and sets it to 1 in a single operation.
// Returns true if the bit was already set, false if it was newly set.
func getAndSetBit(array []uint64, index uint64) bool {
	longIdx := index >> 6
	bitIdx := index & 0x3F
	mask := uint64(1) << bitIdx
	wasSet := (array[longIdx] & mask) != 0
	array[longIdx] |= mask
	return wasSet
}

// countBitsSet counts the number of bits set to 1 in the array.
func countBitsSet(array []uint64) uint64 {
	count := uint64(0)
	for _, val := range array {
		count += uint64(bits.OnesCount64(val))
	}
	return count
}

// unionWith performs a bitwise OR operation between target and source arrays.
// The result is stored in target. Returns the number of bits set in the result.
func unionWith(target, source []uint64) uint64 {
	count := uint64(0)
	for i := range target {
		target[i] |= source[i]
		count += uint64(bits.OnesCount64(target[i]))
	}
	return count
}

// intersect performs a bitwise AND operation between target and source arrays.
// The result is stored in target. Returns the number of bits set in the result.
func intersect(target, source []uint64) uint64 {
	count := uint64(0)
	for i := range target {
		target[i] &= source[i]
		count += uint64(bits.OnesCount64(target[i]))
	}
	return count
}

// invert performs a bitwise NOT operation on the array.
// Returns the number of bits set in the result.
func invert(array []uint64) uint64 {
	count := uint64(0)
	for i := range array {
		array[i] = ^array[i]
		count += uint64(bits.OnesCount64(array[i]))
	}
	return count
}
