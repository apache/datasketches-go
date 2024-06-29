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

const (
	C1 = 0x87c37b91114253d5
	C2 = 0x4cf5ad432745937f
)

type SimpleMurmur3 struct {
	h1 uint64
	h2 uint64
}

func HashInt64SliceMurmur3(key []int64, offsetLongs int, lengthLongs int, seed uint64) (uint64, uint64) {
	hashState := SimpleMurmur3{h1: seed, h2: seed}

	// Number of full 128-bit blocks of 2 longs (the body).
	// Possible exclusion of a remainder of 1 long.
	nblocks := lengthLongs >> 1 //longs / 2

	// Process the 128-bit blocks (the body) into the hash
	for i := 0; i < nblocks; i++ {
		k1 := uint64(key[offsetLongs+(i<<1)])   //offsetLongs + 0, 2, 4, ...
		k2 := uint64(key[offsetLongs+(i<<1)+1]) //offsetLongs + 1, 3, 5, ...
		hashState.blockMix128(k1, k2)
	}

	// Get the tail index wrt hashed portion, remainder length
	tail := nblocks << 1      // 2 longs / block
	rem := lengthLongs - tail // remainder longs: 0,1

	// Get the tail
	k1 := uint64(0)
	if rem != 0 {
		k1 = uint64(key[offsetLongs+tail]) //k2 -> 0
	}

	return hashState.finalMix128(k1, 0, uint64(lengthLongs)<<3)
}

func mixK1(k1 uint64) uint64 {
	k1 *= C1
	k1 = (k1 << 31) | (k1 >> (64 - 31))
	k1 *= C2
	return k1

}

func mixK2(k2 uint64) uint64 {
	k2 *= C2
	k2 = (k2 << 33) | (k2 >> (64 - 33))
	k2 *= C1
	return k2
}

func finalMix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h

}

func (m *SimpleMurmur3) blockMix128(k1, k2 uint64) {
	m.h1 ^= mixK1(k1)
	m.h1 = (m.h1 << 27) | (m.h1 >> (64 - 27))
	m.h1 += m.h2
	m.h1 = m.h1*5 + 0x52dce729

	m.h2 ^= mixK2(k2)
	m.h2 = (m.h2 << 31) | (m.h2 >> (64 - 31))
	m.h2 += m.h1
	m.h2 = m.h2*5 + 0x38495ab5
}

func (m *SimpleMurmur3) finalMix128(k1, k2, inputLengthBytes uint64) (uint64, uint64) {
	m.h1 ^= mixK1(k1)
	m.h2 ^= mixK2(k2)
	m.h1 ^= inputLengthBytes
	m.h2 ^= inputLengthBytes
	m.h1 += m.h2
	m.h2 += m.h1
	m.h1 = finalMix64(m.h1)
	m.h2 = finalMix64(m.h2)
	m.h1 += m.h2
	m.h2 += m.h1
	return m.h1, m.h2
}
