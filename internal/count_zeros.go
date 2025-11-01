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

package internal

var byteLeadingZerosTable = [256]uint8{
	8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

var byteTrailingZerosTable = [256]uint8{
	8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
}

const (
	fclzMask56 uint64 = 0x00ffffffffffffff
	fclzMask48 uint64 = 0x0000ffffffffffff
	fclzMask40 uint64 = 0x000000ffffffffff
	fclzMask32 uint64 = 0x00000000ffffffff
	fclzMask24 uint64 = 0x0000000000ffffff
	fclzMask16 uint64 = 0x000000000000ffff
	fclzMask08 uint64 = 0x00000000000000ff
)

func CountLeadingZerosInU64(input uint64) uint8 {
	if input > fclzMask56 {
		return byteLeadingZerosTable[(input>>56)&fclzMask08]
	}
	if input > fclzMask48 {
		return 8 + byteLeadingZerosTable[(input>>48)&fclzMask08]
	}
	if input > fclzMask40 {
		return 16 + byteLeadingZerosTable[(input>>40)&fclzMask08]
	}
	if input > fclzMask32 {
		return 24 + byteLeadingZerosTable[(input>>32)&fclzMask08]
	}
	if input > fclzMask24 {
		return 32 + byteLeadingZerosTable[(input>>24)&fclzMask08]
	}
	if input > fclzMask16 {
		return 40 + byteLeadingZerosTable[(input>>16)&fclzMask08]
	}
	if input > fclzMask08 {
		return 48 + byteLeadingZerosTable[(input>>8)&fclzMask08]
	}
	return 56 + byteLeadingZerosTable[input&fclzMask08]
}

func CountLeadingZerosInU32(input uint32) uint8 {
	if input > uint32(fclzMask24) {
		return byteLeadingZerosTable[(input>>24)&uint32(fclzMask08)]
	}
	if input > uint32(fclzMask16) {
		return 8 + byteLeadingZerosTable[(input>>16)&uint32(fclzMask08)]
	}
	if input > uint32(fclzMask08) {
		return 16 + byteLeadingZerosTable[(input>>8)&uint32(fclzMask08)]
	}
	return 24 + byteLeadingZerosTable[input&uint32(fclzMask08)]
}

func CountTrailingZerosInU32(input uint32) uint8 {
	for i := 0; i < 4; i++ {
		b := input & 0xff
		if b != 0 {
			return uint8((i << 3) + int(byteTrailingZerosTable[b]))
		}
		input >>= 8
	}
	return 32
}

func CountTrailingZerosInU64(input uint64) uint8 {
	for i := 0; i < 8; i++ {
		b := input & 0xff
		if b != 0 {
			return uint8((i << 3) + int(byteTrailingZerosTable[b]))
		}
		input >>= 8
	}
	return 64
}
