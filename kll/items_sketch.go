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

package kll

import (
	"fmt"
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
	"sort"
)

type ItemSketchOp[C comparable] interface {
	identity() C
	lessFn() common.LessFn[C]
}

type ItemsSketch[C comparable] struct {
	k                 uint16
	m                 uint8
	minK              uint16
	numLevels         uint8
	isLevelZeroSorted bool
	n                 uint64
	levels            []uint32
	items             []C
	itemsSize         uint32
	minItem           *C
	maxItem           *C
	sortedView        *itemsSketchSortedView[C]
	itemsSketchOp     ItemSketchOp[C]
}

const (
	_DEFAULT_K = uint16(200)
	_DEFAULT_M = uint8(8)
	_MIN_K     = uint16(_DEFAULT_M)
	_MAX_K     = (1 << 16) - 1
)

var (
	powersOfThree = []uint64{1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
		1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
		3486784401, 10460353203, 31381059609, 94143178827, 282429536481,
		847288609443, 2541865828329, 7625597484987, 22876792454961, 68630377364883,
		205891132094649}
)

func NewItemsSketch[C comparable](k uint16, itemSketchOp ItemSketchOp[C]) (*ItemsSketch[C], error) {
	if k < _MIN_K || k > _MAX_K {
		return nil, fmt.Errorf("k must be >= %d and <= %d: %d", _MIN_K, _MAX_K, k)
	}
	return &ItemsSketch[C]{
		k,
		_DEFAULT_M,
		k,
		uint8(1),
		false,
		0,
		[]uint32{uint32(k), uint32(k)},
		make([]C, k),
		uint32(k),
		nil,
		nil,
		nil,
		itemSketchOp,
	}, nil
}

func (s *ItemsSketch[C]) IsEmpty() bool {
	return s.n == 0
}

func (s *ItemsSketch[C]) GetN() uint64 {
	return s.n
}

func (s *ItemsSketch[C]) GetNumRetained() uint32 {
	return s.levels[s.numLevels] - s.levels[0]
}

func (s *ItemsSketch[C]) GetMinItem() (C, error) {
	if s.IsEmpty() {
		return s.itemsSketchOp.identity(), fmt.Errorf("operation is undefined for an empty sketch")
	}
	return *s.minItem, nil
}

func (s *ItemsSketch[C]) GetMaxItem() (C, error) {
	if s.IsEmpty() {
		return s.itemsSketchOp.identity(), fmt.Errorf("operation is undefined for an empty sketch")
	}
	return *s.maxItem, nil
}

func (s *ItemsSketch[C]) IsEstimationMode() bool {
	return s.numLevels > 1
}

func (s *ItemsSketch[C]) IsLevelZeroSorted() bool {
	return s.isLevelZeroSorted
}

func (s *ItemsSketch[C]) GetTotalItemsArray() []C {
	if s.n == 0 {
		return make([]C, s.k)
	}
	outArr := make([]C, len(s.items))
	copy(outArr, s.items)
	return outArr
}

func (s *ItemsSketch[C]) GetRank(item C, inclusive bool) (float64, error) {
	if s.IsEmpty() {
		return 0, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return 0, err
	}
	return s.sortedView.GetRank(item, inclusive)
}

func (s *ItemsSketch[C]) GetRanks(item []C, inclusive bool) ([]float64, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	ranks := make([]float64, len(item))
	for i := range item {
		ranks[i], err = s.sortedView.GetRank(item[i], inclusive)
		if err != nil {
			return nil, err
		}
	}
	return ranks, nil
}

/*
  @Override
  public double[] getRanks(final T[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = kllItemsSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }
*/

func (s *ItemsSketch[C]) GetQuantile(rank float64, inclusive bool) (C, error) {
	if s.IsEmpty() {
		return s.itemsSketchOp.identity(), fmt.Errorf("operation is undefined for an empty sketch")
	}
	if rank < 0.0 || rank > 1.0 {
		return s.itemsSketchOp.identity(), fmt.Errorf("normalized rank cannot be less than zero or greater than 1.0: %f", rank)
	}
	err := s.setupSortedView()
	if err != nil {
		return s.itemsSketchOp.identity(), err
	}
	return s.sortedView.GetQuantile(rank, inclusive)
}

func (s *ItemsSketch[C]) GetQuantiles(ranks []float64, inclusive bool) ([]C, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	quantiles := make([]C, len(ranks))
	for i := range ranks {
		quantiles[i], err = s.sortedView.GetQuantile(ranks[i], inclusive)
		if err != nil {
			return nil, err
		}
	}
	return quantiles, nil
}

func (s *ItemsSketch[C]) GetPMF(splitPoints []C, inclusive bool) ([]float64, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	return s.sortedView.GetPMF(splitPoints, inclusive)
}

func (s *ItemsSketch[C]) GetCDF(splitPoints []C, inclusive bool) ([]float64, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	return s.sortedView.GetCDF(splitPoints, inclusive)
}

func (s *ItemsSketch[C]) Update(item C) {
	s.updateItem(item, s.itemsSketchOp.lessFn())
	s.sortedView = nil
}

func (s *ItemsSketch[C]) getLevelsArray() []uint32 {
	levels := make([]uint32, len(s.levels))
	copy(levels, s.levels)
	return levels
}

func (s *ItemsSketch[C]) setupSortedView() error {
	if s.sortedView == nil {
		sView, err := newItemsSketchSortedView[C](s)
		if err != nil {
			return err
		}
		s.sortedView = sView
	}
	return nil
}

func (s *ItemsSketch[C]) updateItem(item C, lessFn common.LessFn[C]) {
	if internal.IsNil(item) {
		return
	}
	if s.IsEmpty() {
		s.minItem = &item
		s.maxItem = &item
	} else {
		if lessFn(item, *s.maxItem) {
			s.maxItem = &item
		}
		if lessFn(*s.maxItem, item) {
			s.maxItem = &item
		}
	}
	level0space := s.levels[0]
	if level0space == 0 {
		s.compressWhileUpdatingSketch()
		level0space = s.levels[0]
	}
	s.n++
	s.isLevelZeroSorted = false
	nextPos := level0space - 1
	s.levels[0] = nextPos
	s.items[nextPos] = item
}

func (s *ItemsSketch[C]) compressWhileUpdatingSketch() {
	level := findLevelToCompact(s.k, s.m, s.numLevels, s.levels)
	if level == s.numLevels-1 {
		//The level to compact is the top level, thus we need to add a level.
		//Be aware that this operation grows the items array,
		//shifts the items data and the level boundaries of the data,
		//and grows the levels array and increments numLevels_.
		s.addEmptyTopLevelToCompletelyFullSketch()
	}
	myLevelsArr := s.levels
	rawBeg := myLevelsArr[level]
	rawEnd := myLevelsArr[level+1]
	// +2 is OK because we already added a new top level if necessary
	popAbove := myLevelsArr[level+2] - rawEnd
	rawPop := rawEnd - rawBeg
	oddPop := rawPop%2 == 1
	adjBeg := rawBeg
	if oddPop {
		adjBeg++
	}
	adjPop := rawPop
	if oddPop {
		adjPop--
	}
	halfAdjPop := adjPop / 2

	//the following is specific to generic Items
	myItemsArr := s.GetTotalItemsArray()
	if level == 0 { // level zero might not be sorted, so we must sort it if we wish to compact it
		lessFn := s.itemsSketchOp.lessFn()
		tmpSlice := myItemsArr[adjBeg : adjBeg+adjPop]
		sort.Slice(tmpSlice, func(a, b int) bool {
			return lessFn(tmpSlice[a], tmpSlice[b])
		})
	}
	if popAbove == 0 {
		randomlyHalveUpItems(myItemsArr, adjBeg, adjPop)
	} else {
		randomlyHalveDownItems(myItemsArr, adjBeg, adjPop)
		mergeSortedItemsArrays(
			myItemsArr, adjBeg, halfAdjPop,
			myItemsArr, rawEnd, popAbove,
			myItemsArr, adjBeg+halfAdjPop, s.itemsSketchOp.lessFn())
	}
	newIndex := myLevelsArr[level+1] - halfAdjPop // adjust boundaries of the level above
	s.levels[level+1] = newIndex

	if oddPop {
		s.levels[level] = myLevelsArr[level+1] - 1          // the current level now contains one item
		myItemsArr[myLevelsArr[level]] = myItemsArr[rawBeg] // namely this leftover guy
	} else {
		s.levels[level] = myLevelsArr[level+1] // the current level is now empty
	}

	if level > 0 {
		amount := rawBeg - myLevelsArr[0] // adjust boundary

		for i := amount; i > 0; i-- {
			// Start from the end as we are shifting to the right,
			// failing to do so will corrupt the items array.
			tgtInx := myLevelsArr[0] + halfAdjPop + i - 1
			stcInx := myLevelsArr[0] + i - 1
			myItemsArr[tgtInx] = myItemsArr[stcInx]
		}
	}
	for lvl := uint8(0); lvl < level; lvl++ {
		newIndex = myLevelsArr[lvl] + halfAdjPop //adjust boundary
		s.levels[lvl] = newIndex
	}
	s.items = myItemsArr
}

func (s *ItemsSketch[C]) addEmptyTopLevelToCompletelyFullSketch() {
	myCurLevelsArr := s.getLevelsArray()
	myCurNumLevels := s.numLevels
	myCurTotalItemsCapacity := myCurLevelsArr[myCurNumLevels]

	myCurItemsArr := s.GetTotalItemsArray()
	minItem := s.minItem
	maxItem := s.maxItem

	deltaItemsCap := levelCapacity(s.k, myCurNumLevels+1, 0, s.m)
	myNewTotalItemsCapacity := myCurTotalItemsCapacity + deltaItemsCap

	// Check if growing the levels arr if required.
	// Note that merging MIGHT over-grow levels_, in which case we might not have to grow it
	growLevelsArr := len(myCurLevelsArr) < int(myCurNumLevels+2)

	var (
		myNewLevelsArr []uint32
		myNewNumLevels uint8
	)

	//myNewLevelsArr := make([]uint32, myCurNumLevels+2)
	// GROW LEVELS ARRAY
	if growLevelsArr {
		//grow levels arr by one and copy the old data to the new array, extra space at the top.
		myNewLevelsArr = make([]uint32, myCurNumLevels+2)
		copy(myNewLevelsArr, myCurLevelsArr)
		myNewNumLevels = myCurNumLevels + 1
		s.numLevels++ //increment for off-heap
	} else {
		myNewLevelsArr = myCurLevelsArr
		myNewNumLevels = myCurNumLevels
	}

	// This loop updates all level indices EXCLUDING the "extra" index at the top
	for level := uint8(0); level <= myNewNumLevels-1; level++ {
		myNewLevelsArr[level] += deltaItemsCap
	}
	myNewLevelsArr[myNewNumLevels] = myNewTotalItemsCapacity // initialize the new "extra" index at the top

	// GROW items ARRAY
	myNewItemsArr := make([]C, myNewTotalItemsCapacity)
	for i := uint32(0); i < myCurTotalItemsCapacity; i++ {
		myNewItemsArr[i+deltaItemsCap] = myCurItemsArr[i]
	}

	// update our sketch with new expanded spaces
	s.numLevels = myNewNumLevels
	s.levels = myNewLevelsArr

	s.minItem = minItem
	s.maxItem = maxItem
	s.items = myNewItemsArr
}

func findLevelToCompact(k uint16, m uint8, numLevels uint8, levels []uint32) uint8 {
	level := uint8(0)
	for {
		pop := levels[level+1] - levels[level]
		capacity := levelCapacity(k, numLevels, level, m)
		if pop >= capacity {
			return level
		}
		level++
	}
}

func levelCapacity(k uint16, numLevels uint8, level uint8, m uint8) uint32 {
	depth := numLevels - level - 1
	return max(uint32(m), intCapAux(k, depth))
}

func intCapAux(k uint16, depth uint8) uint32 {
	if depth <= 30 {
		return intCapAuxAux(k, depth)
	}
	half := depth / 2
	rest := depth - half
	tmp := intCapAuxAux(k, half)
	return intCapAuxAux(uint16(tmp), rest)
}

func intCapAuxAux(k uint16, depth uint8) uint32 {
	twok := uint64(k << 1)                        // for rounding at the end, pre-multiply by 2 here, divide by 2 during rounding.
	tmp := (twok << depth) / powersOfThree[depth] //2k* (2/3)^depth. 2k also keeps the fraction larger.
	result := (tmp + 1) >> 1                      // (tmp + 1)/2. If odd, round up. This guarantees an integer.
	if result <= uint64(k) {
		return uint32(result)
	}
	return uint32(k)
}

func randomlyHalveUpItems[C comparable](buf []C, start uint32, length uint32) {
	halfLength := length / 2
	//offset := rand.Intn(2)
	offset := 1
	j := (start + length) - 1 - uint32(offset)
	for i := (start + length) - 1; i >= (start + halfLength); i-- {
		buf[i] = buf[j]
		j -= 2
	}
}

func randomlyHalveDownItems[C comparable](buf []C, start uint32, length uint32) {
	halfLength := length / 2
	//offset := rand.Intn(2)
	offset := 1
	j := start + uint32(offset)
	for i := start; i < (start + halfLength); i++ {
		buf[i] = buf[j]
		j += 2
	}
}

func mergeSortedItemsArrays[C comparable](bufA []C, startA uint32, lenA uint32,
	bufB []C, startB uint32, lenB uint32,
	bufC []C, startC uint32, lessFn common.LessFn[C]) {
	lenC := lenA + lenB
	limA := startA + lenA
	limB := startB + lenB
	limC := startC + lenC

	a := startA
	b := startB

	for c := startC; c < limC; c++ {
		if a == limA {
			bufC[c] = bufB[b]
			b++
		} else if b == limB {
			bufC[c] = bufA[a]
			a++
		} else if lessFn(bufA[a], bufB[b]) {
			bufC[c] = bufA[a]
			a++
		} else {
			bufC[c] = bufB[b]
			b++
		}
	}
}