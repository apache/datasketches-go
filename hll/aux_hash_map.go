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

package hll

import (
	"encoding/binary"
	"fmt"
)

type auxHashMap interface {
	mustFindValueFor(slotNo int) int
	mustReplace(slotNo int, value int)
	mustAdd(slotNo int, value int)
	iterator() pairIterator
	getAuxCount() int
	getLgAuxArrInts() int
	copy() auxHashMap
	getUpdatableSizeBytes() int
	getCompactSizeBytes() int
	getAuxIntArr() []int
}

// auxHashMapImpl is a hash table for the Aux array.
type auxHashMapImpl struct {
	lgConfigK    int //required for #slot bits
	lgAuxArrInts int
	auxCount     int
	auxIntArr    []int
}

func (a *auxHashMapImpl) copy() auxHashMap {
	newA := a
	newA.auxIntArr = make([]int, len(a.auxIntArr))
	copy(newA.auxIntArr, a.auxIntArr)
	return newA
}

// newAuxHashMap returns a new auxHashMap.
func newAuxHashMap(lgAuxArrInts int, lgConfigK int) auxHashMap {
	return &auxHashMapImpl{
		lgConfigK:    lgConfigK,
		lgAuxArrInts: lgAuxArrInts,
		auxCount:     0,
		auxIntArr:    make([]int, 1<<lgAuxArrInts),
	}
}

// deserializeAuxHashMap returns a new auxHashMap from the given byte array.
func deserializeAuxHashMap(byteArray []byte, offset int, lgConfigL int, auxCount int, srcCompact bool) auxHashMap {
	var (
		lgAuxArrInts int
	)

	if srcCompact {
		lgAuxArrInts = computeLgArr(byteArray, auxCount, lgConfigL)
	} else {
		lgAuxArrInts = extractLgArr(byteArray)
	}

	auxMap := newAuxHashMap(lgAuxArrInts, lgConfigL)
	configKMask := (1 << lgConfigL) - 1

	if srcCompact {
		for i := 0; i < auxCount; i++ {
			pair := int(binary.LittleEndian.Uint32(byteArray[offset+(i<<2) : offset+(i<<2)+4]))
			slotNo := getPairLow26(pair) & configKMask
			value := getPairValue(pair)
			auxMap.mustAdd(slotNo, value) //increments count
		}
	} else { //updatable
		auxArrInts := 1 << lgAuxArrInts
		for i := 0; i < auxArrInts; i++ {
			pair := int(binary.LittleEndian.Uint32(byteArray[offset+(i<<2) : offset+(i<<2)+4]))
			if pair == empty {
				continue
			}
			slotNo := getPairLow26(pair) & configKMask
			value := getPairValue(pair)
			auxMap.mustAdd(slotNo, value) //increments count
		}
	}
	return auxMap
}

func (a *auxHashMapImpl) getAuxIntArr() []int {
	return a.auxIntArr
}

func (a *auxHashMapImpl) getCompactSizeBytes() int {
	return a.auxCount << 2
}

func (a *auxHashMapImpl) getUpdatableSizeBytes() int {
	return 4 << a.lgAuxArrInts
}

func (a *auxHashMapImpl) mustFindValueFor(slotNo int) int {
	index := findAuxHashMap(a.auxIntArr, a.lgAuxArrInts, a.lgConfigK, slotNo)
	if index < 0 {
		panic(fmt.Sprintf("SlotNo not found: %d", slotNo))
	}
	return getPairValue(a.auxIntArr[index])
}

func (a *auxHashMapImpl) mustReplace(slotNo int, value int) {
	index := findAuxHashMap(a.auxIntArr, a.lgAuxArrInts, a.lgConfigK, slotNo)
	if index < 0 {
		pairStr := pairString(pair(slotNo, value))
		panic(fmt.Sprintf("pair not found: %v", pairStr))
	}
	a.auxIntArr[index] = pair(slotNo, value)
}

// mustAdd adds the slotNo and value to the aux array.
// slotNo the index from the HLL array
// value the HLL value at the slotNo.
func (a *auxHashMapImpl) mustAdd(slotNo int, value int) {
	index := findAuxHashMap(a.auxIntArr, a.lgAuxArrInts, a.lgConfigK, slotNo)
	pair := pair(slotNo, value)
	if index >= 0 {
		pairStr := pairString(pair)
		panic(fmt.Sprintf("found a slotNo that should not be there: %s", pairStr))
	}
	a.auxIntArr[^index] = pair
	a.auxCount++
	a.checkGrow()
}

func (a *auxHashMapImpl) getLgAuxArrInts() int {
	return a.lgAuxArrInts
}

// iterator returns an iterator over the Aux array.
func (a *auxHashMapImpl) iterator() pairIterator {
	return newIntArrayPairIterator(a.auxIntArr, a.lgConfigK)
}

// getAuxCount returns the number of entries in the Aux array.
func (a *auxHashMapImpl) getAuxCount() int {
	return a.auxCount
}

// checkGrow checks to see if the aux array should be grown and does so if needed.
func (a *auxHashMapImpl) checkGrow() {
	if (resizeDenom * a.auxCount) <= (resizeNumber * len(a.auxIntArr)) {
		return
	}
	a.growAuxSpace()
}

// growAuxSpace doubles the size of the aux array and reinsert the existing entries.
func (a *auxHashMapImpl) growAuxSpace() {
	oldArray := a.auxIntArr
	configKMask := int((1 << a.lgConfigK) - 1)
	a.lgAuxArrInts++
	a.auxIntArr = make([]int, 1<<a.lgAuxArrInts)
	for _, fetched := range oldArray {
		if fetched != empty {
			//find empty in new array
			idx := findAuxHashMap(a.auxIntArr, a.lgAuxArrInts, a.lgConfigK, fetched&configKMask)
			a.auxIntArr[^idx] = fetched
		}
	}
}

// findAuxHashMap searches the Aux arr hash table for an empty or a matching slotNo depending on the context.
// If entire entry is empty, returns one's complement of index = found empty.
// If entry contains given slotNo, returns its index = found slotNo.
// Continues searching.
// If the probe comes back to original index, panic.
func findAuxHashMap(auxArr []int, lgAuxArrInts int, lgConfigK int, slotNo int) int {
	if lgAuxArrInts >= lgConfigK {
		panic("lgAuxArrInts >= lgConfigK")
	}
	auxArrMask := (1 << lgAuxArrInts) - 1
	configKMask := (1 << lgConfigK) - 1
	probe := slotNo & auxArrMask
	loopIndex := probe
	for {
		arrVal := auxArr[probe]
		if arrVal == empty {
			return ^probe
		} else if slotNo == (arrVal & configKMask) {
			return probe
		}
		stride := (slotNo >> lgAuxArrInts) | 1
		probe = (probe + stride) & auxArrMask
		if probe == loopIndex {
			panic("key not found and no empty slots")
		}
	}
}
