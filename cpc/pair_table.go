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

package cpc

import (
	"fmt"
	"slices"
)

const (
	upsizeNumer   = 3
	upsizeDenom   = 4
	downsizeNumer = 1
	downsizeDenom = 4
)

type pairTable struct {
	lgSizeInts int
	validBits  int
	numPairs   int
	slotsArr   []int
}

func NewPairTable(lgSizeInts, numValidBits int) (*pairTable, error) {
	if err := checkLgSizeInts(lgSizeInts); err != nil {
		return nil, err
	}
	numSlots := 1 << lgSizeInts
	validBits := numValidBits
	numPairs := 0
	slotsArr := make([]int, numSlots)
	for i := range slotsArr {
		slotsArr[i] = -1
	}
	return &pairTable{lgSizeInts, validBits, numPairs, slotsArr}, nil
}

func (p *pairTable) clear() {
	for i := range p.slotsArr {
		p.slotsArr[i] = -1
	}
	p.numPairs = 0
}

func (p *pairTable) maybeInsert(item int) (bool, error) {
	//SHARED CODE (implemented as a macro in C and expanded here)
	lgSizeInts := p.lgSizeInts
	sizeInts := 1 << lgSizeInts
	mask := sizeInts - 1
	shift := p.validBits - lgSizeInts
	//rtAssert(shift > 0)
	probe := item >> shift
	//rtAssert((probe >= 0) && (probe <= mask))
	fetched := p.slotsArr[probe]
	for fetched != item && fetched != -1 {
		probe = (probe + 1) & mask
		fetched = p.slotsArr[probe]
	}
	//END SHARED CODE
	if fetched == item {
		return false, nil
	} else {
		//assert (fetched == -1)
		p.slotsArr[probe] = item
		p.numPairs++
		for (upsizeDenom * p.numPairs) > (upsizeNumer * (1 << p.lgSizeInts)) {
			if err := p.rebuild(p.lgSizeInts + 1); err != nil {
				return false, err
			}

		}
		return true, nil
	}
}

func (p *pairTable) maybeDelete(item int) (bool, error) {
	lgSizeInts := p.lgSizeInts
	sizeInts := 1 << lgSizeInts
	mask := sizeInts - 1
	shift := p.validBits - lgSizeInts
	//rtAssert(shift > 0)
	probe := item >> shift
	//rtAssert((probe >= 0) && (probe <= mask))
	arr := p.slotsArr
	fetched := arr[probe]
	for fetched != item && fetched != -1 {
		probe = (probe + 1) & mask
		fetched = arr[probe]
	}
	//END SHARED CODE
	if fetched == -1 {
		return false, nil
	} else {
		//assert (fetched == item)
		// delete the item
		arr[probe] = -1
		p.numPairs--
		// re-insert all items between the freed slot and the next empty slot
		probe = (probe + 1) & mask
		fetched = arr[probe]
		for fetched != -1 {
			arr[probe] = -1
			if _, err := p.maybeInsert(fetched); err != nil {
				return false, err
			}
			probe = (probe + 1) & mask
			fetched = arr[probe]
		}
		// shrink if necessary
		for (downsizeDenom*p.numPairs) < (downsizeNumer*(1<<p.lgSizeInts)) && p.lgSizeInts > 2 {
			if err := p.rebuild(p.lgSizeInts - 1); err != nil {
				return false, err
			}
		}
		return true, nil
	}

}

func (p *pairTable) mustInsert(item int) {
	//SHARED CODE (implemented as a macro in C and expanded here)
	lgSizeInts := p.lgSizeInts
	sizeInts := 1 << lgSizeInts
	mask := sizeInts - 1
	shift := p.validBits - lgSizeInts
	//rtAssert(shift > 0)
	probe := item >> shift
	//rtAssert((probe >= 0) && (probe <= mask))
	arr := p.slotsArr
	fetched := arr[probe]
	for fetched != item && fetched != -1 {
		probe = (probe + 1) & mask
		fetched = arr[probe]
	}
	//END SHARED CODE
	if fetched == item {
		panic("PairTable mustInsert() failed")
	} else {
		//assert (fetched == -1)
		arr[probe] = item
		// counts and resizing must be handled by the caller.
	}
}

func (p *pairTable) rebuild(newLgSizeInts int) error {
	if err := checkLgSizeInts(newLgSizeInts); err != nil {
		return err
	}
	newSize := 1 << newLgSizeInts
	oldSize := 1 << p.lgSizeInts
	if newSize <= p.numPairs {
		fmt.Errorf("newSize <= numPairs")
	}
	oldSlotsArr := p.slotsArr
	p.slotsArr = make([]int, newSize)
	for i := range p.slotsArr {
		p.slotsArr[i] = -1
	}
	p.lgSizeInts = newLgSizeInts
	for i := 0; i < oldSize; i++ {
		item := oldSlotsArr[i]
		if item != -1 {
			p.mustInsert(item)
		}
	}
	return nil
}

func introspectiveInsertionSort(a []int, l, r int) {
	length := (r - l) + 1
	cost := 0
	costLimit := 8 * length
	for i := l + 1; i <= r; i++ {
		j := i
		v := int64(a[i]) & 0xFFFF_FFFF
		for j >= (l+1) && v < (int64(a[j-1])&0xFFFF_FFFF) {
			a[j] = a[j-1]
			j--
		}
		a[j] = int(v)
		cost += i - j
		if cost > costLimit {
			b := make([]int, len(a))
			for m := 0; m < len(a); m++ {
				b[m] = a[m] & 0xFFFF_FFFF
			}
			slices.Sort(b[j : r+1])
			for m := 0; m < len(a); m++ {
				a[m] = b[m]
			}
			return
		}
	}
}

func mergePairs(arrA []int, startA, lengthA int, arrB []int, startB, lengthB int, arrC []int, startC int) {
	lengthC := lengthA + lengthB
	limA := startA + lengthA
	limB := startB + lengthB
	limC := startC + lengthC
	a := startA
	b := startB
	c := startC
	for c < limC {
		if b >= limB {
			arrC[c] = arrA[a]
			a++
		} else if a >= limA {
			arrC[c] = arrB[b]
			b++
		} else {
			aa := int64(arrA[a]) & 0xFFFF_FFFF
			bb := int64(arrB[b]) & 0xFFFF_FFFF
			if aa < bb {
				arrC[c] = arrA[a]
				a++
			} else {
				arrC[c] = arrB[b]
				b++
			}
		}
		c++
	}
}

// copy creates and returns a deep copy of the pairTable.
func (p *pairTable) copy() (*pairTable, error) {
	// Create a new pairTable using the same lgSizeInts and validBits.
	newPT, err := NewPairTable(p.lgSizeInts, p.validBits)
	if err != nil {
		// This should not happen if p is valid.
		return nil, err
	}
	// copy the number of pairs.
	newPT.numPairs = p.numPairs
	// Deep copy the slots array.
	newPT.slotsArr = make([]int, len(p.slotsArr))
	copy(newPT.slotsArr, p.slotsArr)
	return newPT, nil
}

// unwrap extracts the valid items from the pair table using the unwrapping logic.
func (p *pairTable) unwrap(numPairs int) ([]int, error) {
	if numPairs < 1 {
		return nil, nil
	}

	tableSize := 1 << p.lgSizeInts
	result := make([]int, numPairs)

	i, l, r := 0, 0, numPairs-1
	hiBit := 1 << (p.validBits - 1) // Highest bit based on validBits.

	// Process the region before the first empty slot (-1).
	for i < tableSize && p.slotsArr[i] != -1 {
		item := p.slotsArr[i]
		i++
		// If the high bit is set, treat as wrapped item and place at the end.
		if (item & hiBit) != 0 {
			result[r] = item
			r--
		} else {
			result[l] = item
			l++
		}
	}

	// Process the rest of the table normally.
	for i < tableSize {
		look := p.slotsArr[i]
		i++
		if look != -1 {
			result[l] = look
			l++
		}
	}

	// Check that we've distributed items exactly.
	if l != (r + 1) {
		return nil, fmt.Errorf("unwrap: inconsistent indices (l=%d, r=%d)", l, r)
	}

	return result, nil
}
