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

import "encoding/binary"

// Preamble constants matching C++ implementation
const (
	// Header sizes
	preambleBytes      = 32 // Full preamble size for non-empty filter
	preambleEmptyBytes = 24 // Preamble size for empty filter

	// Preamble field offsets
	preambleLongsOffset  = 0
	serVerOffset         = 1
	familyIDOffset       = 2
	flagsOffset          = 3
	numHashesOffset      = 4
	seedOffset           = 8
	bitArrayLengthOffset = 16
	numBitsSetOffset     = 24
	bitArrayOffset       = 32

	// Family and version identifiers
	familyID = 21
	serVer   = 1

	// Preamble sizes in longs
	preambleLongsEmpty    = 3
	preambleLongsStandard = 4

	// Flag masks
	emptyFlagMask = 0x04

	// Special values
	dirtyBitsValue = 0xFFFFFFFFFFFFFFFF
)

// extractPreambleLongs extracts the preamble longs field from the header.
func extractPreambleLongs(bytes []byte) uint8 {
	return bytes[preambleLongsOffset]
}

// extractSerVer extracts the serialization version from the header.
func extractSerVer(bytes []byte) uint8 {
	return bytes[serVerOffset]
}

// extractFamilyID extracts the family ID from the header.
func extractFamilyID(bytes []byte) uint8 {
	return bytes[familyIDOffset]
}

// extractFlags extracts the flags byte from the header.
func extractFlags(bytes []byte) uint8 {
	return bytes[flagsOffset]
}

// extractNumHashes extracts the number of hash functions from the header.
func extractNumHashes(bytes []byte) uint16 {
	return binary.LittleEndian.Uint16(bytes[numHashesOffset:])
}

// extractSeed extracts the hash seed from the header.
func extractSeed(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes[seedOffset:])
}

// extractBitArrayLength extracts the bit array length (in longs) from the header.
func extractBitArrayLength(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes[bitArrayLengthOffset:])
}

// extractNumBitsSet extracts the number of bits set from the header.
// Only valid for non-empty filters.
func extractNumBitsSet(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes[numBitsSetOffset:])
}

// insertPreambleLongs inserts the preamble longs field into the header.
func insertPreambleLongs(bytes []byte, val uint8) {
	bytes[preambleLongsOffset] = val
}

// insertSerVer inserts the serialization version into the header.
func insertSerVer(bytes []byte) {
	bytes[serVerOffset] = serVer
}

// insertFamilyID inserts the family ID into the header.
func insertFamilyID(bytes []byte) {
	bytes[familyIDOffset] = familyID
}

// insertFlags inserts the flags byte into the header.
func insertFlags(bytes []byte, flags uint8) {
	bytes[flagsOffset] = flags
}

// insertNumHashes inserts the number of hash functions into the header.
func insertNumHashes(bytes []byte, numHashes uint16) {
	binary.LittleEndian.PutUint16(bytes[numHashesOffset:], numHashes)
}

// insertSeed inserts the hash seed into the header.
func insertSeed(bytes []byte, seed uint64) {
	binary.LittleEndian.PutUint64(bytes[seedOffset:], seed)
}

// insertBitArrayLength inserts the bit array length (in longs) into the header.
func insertBitArrayLength(bytes []byte, length uint32) {
	binary.LittleEndian.PutUint32(bytes[bitArrayLengthOffset:], length)
}

// insertNumBitsSet inserts the number of bits set into the header.
func insertNumBitsSet(bytes []byte, numBitsSet uint64) {
	binary.LittleEndian.PutUint64(bytes[numBitsSetOffset:], numBitsSet)
}

// isEmptyFlag checks if the empty flag is set in the flags byte.
func isEmptyFlag(flags uint8) bool {
	return (flags & emptyFlagMask) != 0
}

// setEmptyFlag sets the empty flag in the flags byte.
func setEmptyFlag(flags uint8) uint8 {
	return flags | emptyFlagMask
}

// clearEmptyFlag clears the empty flag in the flags byte.
func clearEmptyFlag(flags uint8) uint8 {
	return flags &^ emptyFlagMask
}
