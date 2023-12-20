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

package frequencies

import (
	"encoding/binary"
	"fmt"
	"github.com/apache/datasketches-go/common"
	"math/bits"
	"strconv"
	"strings"
)

type LongSketch struct {
	// Log2 Maximum length of the arrays internal to the hash map supported by the data
	// structure.
	lgMaxMapSize int
	// The current number of counters supported by the hash map.
	curMapCap int //the threshold to purge
	// Tracks the total of decremented counts.
	offset int64
	// The sum of all frequencies of the stream so far.
	streamWeight int64
	// The maximum number of samples used to compute approximate median of counters when doing
	// decrement
	sampleSize int
	// Hash map mapping stored items to approximate counts
	hashMap *reversePurgeLongHashMap
}

const (
	strPreambleTokens = 6
)

/**
 * Construct this sketch with parameter lgMapMapSize and lgCurMapSize. This internal
 * constructor is used when deserializing the sketch.
 *
 * @param lgMaxMapSize Log2 of the physical size of the internal hash map managed by this
 * sketch. The maximum capacity of this internal hash map is 0.75 times 2^lgMaxMapSize.
 * Both the ultimate accuracy and size of this sketch are a function of lgMaxMapSize.
 *
 * @param lgCurMapSize Log2 of the starting (current) physical size of the internal hash
 * map managed by this sketch.
 */

// NewLongSketch returns a new LongSketch with the given lgMaxMapSize and lgCurMapSize.
// lgMaxMapSize is the log2 of the physical size of the internal hash map managed by this
// sketch. The maximum capacity of this internal hash map is 0.75 times 2^lgMaxMapSize.
// Both the ultimate accuracy and size of this sketch are a function of lgMaxMapSize.
// lgCurMapSize is the log2 of the starting (current) physical size of the internal hash
// map managed by this sketch.
func NewLongSketch(lgMaxMapSize int, lgCurMapSize int) (*LongSketch, error) {
	//set initial size of hash map
	lgMaxMapSize = max(lgMaxMapSize, _LG_MIN_MAP_SIZE)
	lgCurMapSize = max(lgCurMapSize, _LG_MIN_MAP_SIZE)
	hashMap, err := NewReversePurgeLongHashMap(1 << lgCurMapSize)
	if err != nil {
		return nil, err
	}
	curMapCap := hashMap.getCapacity()
	maxMapCap := int(float64(uint64(1<<lgMaxMapSize)) * loadFactor)
	offset := int64(0)
	sampleSize := min(sampleSize, maxMapCap)
	return &LongSketch{
		lgMaxMapSize: int(lgMaxMapSize),
		curMapCap:    curMapCap,
		offset:       offset,
		sampleSize:   sampleSize,
		hashMap:      hashMap,
	}, nil
}

// NewLongSketchWithMaxMapSize constructs a new LongSketch with the given maxMapSize and the
// default initialMapSize (8).
// maxMapSize determines the physical size of the internal hash map managed by this
// sketch and must be a power of 2.  The maximum capacity of this internal hash map is
// 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a
// function of maxMapSize.
func NewLongSketchWithMaxMapSize(maxMapSize int) (*LongSketch, error) {
	log2OfInt, err := common.ExactLog2(maxMapSize)
	if err != nil {
		return nil, fmt.Errorf("maxMapSize, %e", err)
	}
	return NewLongSketch(log2OfInt, _LG_MIN_MAP_SIZE)
}

func NewLongSketchFromSlice(slc []byte) (*LongSketch, error) {
	pre0, err := checkPreambleSize(slc)
	if err != nil {
		return nil, err
	}
	maxPreLongs := common.FamilyEnum.Frequency.MaxPreLongs
	preLongs := extractPreLongs(pre0)
	serVer := extractSerVer(pre0)
	familyID := extractFamilyID(pre0)
	lgMaxMapSize := extractLgMaxMapSize(pre0)
	lgCurMapSize := extractLgCurMapSize(pre0)
	empty := (extractFlags(pre0) & _EMPTY_FLAG_MASK) != 0

	// Checks
	preLongsEq1 := preLongs == 1
	preLongsEqMax := preLongs == maxPreLongs
	if !preLongsEq1 && !preLongsEqMax {
		return nil, fmt.Errorf("Possible Corruption: PreLongs must be 1 or %d: %d", maxPreLongs, preLongs)
	}
	if serVer != _SER_VER {
		return nil, fmt.Errorf("Possible Corruption: Ser Ver must be %d: %d", _SER_VER, serVer)
	}
	actFamID := common.FamilyEnum.Frequency.Id
	if familyID != actFamID {
		return nil, fmt.Errorf("Possible Corruption: FamilyID must be %d: %d", actFamID, familyID)
	}
	if empty && !preLongsEq1 {
		return nil, fmt.Errorf("Possible Corruption: Empty Flag set incorrectly: %t", preLongsEq1)
	}
	if empty {
		return NewLongSketch(lgMaxMapSize, _LG_MIN_MAP_SIZE)
	}
	// get full preamble
	preArr := make([]int64, preLongs)
	for i := 0; i < preLongs; i++ {
		preArr[i] = int64(binary.LittleEndian.Uint64(slc[i<<3:]))
	}
	fls, err := NewLongSketch(lgMaxMapSize, lgCurMapSize)
	if err != nil {
		return nil, err
	}
	fls.streamWeight = 0 //update after
	fls.offset = preArr[3]

	preBytes := preLongs << 3
	activeItems := extractActiveItems(preArr[1])

	// Get countArray
	countArray := make([]int64, activeItems)
	reqBytes := preBytes + 2*activeItems*8 //count Arr + Items Arr
	if len(slc) < reqBytes {
		return nil, fmt.Errorf("Possible Corruption: Insufficient bytes in array: %d, %d", len(slc), reqBytes)
	}
	for i := 0; i < activeItems; i++ {
		countArray[i] = int64(binary.LittleEndian.Uint64(slc[preBytes+(i<<3):]))
	}

	// Get itemArray
	itemsOffset := preBytes + (8 * activeItems)
	itemArray := make([]int64, activeItems)
	for i := 0; i < activeItems; i++ {
		itemArray[i] = int64(binary.LittleEndian.Uint64(slc[itemsOffset+(i<<3):]))
	}

	// UpdateMany the sketch
	for i := 0; i < activeItems; i++ {
		fls.UpdateMany(itemArray[i], countArray[i])
	}

	fls.streamWeight = preArr[2] //override streamWeight due to updating
	return fls, nil
}

func NewLongSketchFromString(str string) (*LongSketch, error) {
	if len(str) < 1 {
		return nil, fmt.Errorf("String is empty")
	}
	// Remove trailing comma if present
	// as this will cause a problem with the split
	if str[len(str)-1] == ',' {
		str = str[:len(str)-1]
	}
	tokens := strings.Split(str, ",")
	if len(tokens) < (strPreambleTokens + 2) {
		return nil, fmt.Errorf("String not long enough: %d", len(tokens))
	}
	serVe, err := strconv.ParseInt(tokens[0], 10, 32)
	if err != nil {
		return nil, err
	}
	famID, err := strconv.ParseInt(tokens[1], 10, 32)
	if err != nil {
		return nil, err
	}
	lgMax, err := strconv.ParseInt(tokens[2], 10, 32)
	if err != nil {
		return nil, err
	}
	flags, err := strconv.ParseInt(tokens[3], 10, 32)
	if err != nil {
		return nil, err
	}
	streamWt, err := strconv.ParseInt(tokens[4], 10, 64)
	if err != nil {
		return nil, err
	}
	offset, err := strconv.ParseInt(tokens[5], 10, 64)
	if err != nil {
		return nil, err
	}
	//should always get at least the next 2 from the map
	numActive, err := strconv.ParseInt(tokens[6], 10, 32)
	if err != nil {
		return nil, err
	}
	lgCurOrigin, err := strconv.ParseUint(tokens[7], 10, 32)
	if err != nil {
		return nil, err
	}
	lgCur := bits.TrailingZeros64(lgCurOrigin)

	//checks
	if serVe != _SER_VER {
		return nil, fmt.Errorf("Possible Corruption: Bad SerVer: %d", serVe)
	}
	if famID != int64(common.FamilyEnum.Frequency.Id) {
		return nil, fmt.Errorf("Possible Corruption: Bad Family: %d", famID)
	}
	empty := flags > 0
	if !empty && (numActive == 0) {
		return nil, fmt.Errorf("Possible Corruption: !Empty && NumActive=0;  strLen: %d", numActive)
	}
	numTokens := int64(len(tokens))
	if (2 * numActive) != (numTokens - strPreambleTokens - 2) {
		return nil, fmt.Errorf("Possible Corruption: Incorrect # of tokens: %d, numActive: %d", numTokens, numActive)
	}
	//    if ((2 * numActive) != (numTokens - STR_PREAMBLE_TOKENS - 2)) {
	sk, err := NewLongSketch(int(lgMax), int(lgCur))
	if err != nil {
		return nil, err
	}
	sk.streamWeight = streamWt
	sk.offset = offset
	sk.hashMap, err = deserializeFromStringArray(tokens)
	if err != nil {
		return nil, err
	}
	return sk, nil
}

func (s *LongSketch) getEstimate(item int64) (int64, error) {
	itemCount, err := s.hashMap.get(item)
	if err != nil {
		return 0, err
	}
	return itemCount + s.offset, nil
}

func (s *LongSketch) getLowerBound(item int64) (int64, error) {
	// LB = itemCount
	return s.hashMap.get(item)
}

func (s *LongSketch) getUpperBound(item int64) (int64, error) {
	itemCount, err := s.hashMap.get(item)
	if err != nil {
		return 0, err
	}
	return itemCount + s.offset, nil
}

func (s *LongSketch) getNumActiveItems() int {
	return s.hashMap.numActive
}

// getMaximumMapCapacity returns the maximum number of counters the sketch is configured to
// support.
func (s *LongSketch) getMaximumMapCapacity() int {
	return int(float64(uint64(1<<s.lgMaxMapSize)) * loadFactor)
}

func (s *LongSketch) getStorageBytes() int {
	if s.isEmpty() {
		return 8
	}
	return (4 * 8) + (16 * s.getNumActiveItems())
}

func (s *LongSketch) getCurrentMapCapacity() int {
	return s.curMapCap
}

func (s *LongSketch) getMaximumError() int64 {
	return s.offset
}

func (s *LongSketch) getStreamLength() int64 {
	return s.streamWeight
}

func (s *LongSketch) isEmpty() bool {
	return s.getNumActiveItems() == 0
}

func (s *LongSketch) Update(item int64) error {
	return s.UpdateMany(item, 1)
}

func (s *LongSketch) UpdateMany(item int64, count int64) error {
	if count == 0 {
		return nil
	}
	if count < 0 {
		return fmt.Errorf("count may not be negative")
	}
	s.streamWeight += count
	err := s.hashMap.adjustOrPutValue(item, count)
	if err != nil {
		return err
	}

	if s.hashMap.numActive > s.curMapCap {
		// Over the threshold, we need to do something
		if s.hashMap.lgLength < s.lgMaxMapSize {
			// Below tgt size, we can grow
			s.hashMap.resize(2 * len(s.hashMap.keys))
			s.curMapCap = s.hashMap.getCapacity()
		} else {
			// At tgt size, must purge
			s.offset += s.hashMap.purge(s.sampleSize)
			if s.getNumActiveItems() > s.getMaximumMapCapacity() {
				return fmt.Errorf("purge did not reduce active items")
			}
		}
	}
	return nil
}

func (s *LongSketch) merge(other *LongSketch) (*LongSketch, error) {
	if other == nil || other.isEmpty() {
		return s, nil
	}
	streamWt := s.streamWeight + other.streamWeight //capture before merge
	iter := other.hashMap.iterator()
	for iter.next() {
		err := s.UpdateMany(iter.getKey(), iter.getValue())
		if err != nil {
			return nil, err
		}
	}
	s.offset += other.offset
	s.streamWeight = streamWt //corrected streamWeight
	return s, nil
}

func (s *LongSketch) serializeToString() (string, error) {
	var sb strings.Builder
	//start the string with parameters of the sketch
	serVer := _SER_VER //0
	famID := common.FamilyEnum.Frequency.Id
	lgMaxMapSz := s.lgMaxMapSize
	flags := 0
	if s.hashMap.numActive == 0 {
		flags = _EMPTY_FLAG_MASK
	}
	_, err := fmt.Fprintf(&sb, "%d,%d,%d,%d,%d,%d,", serVer, famID, lgMaxMapSz, flags, s.streamWeight, s.offset)
	if err != nil {
		return "", err
	}
	sb.WriteString(s.hashMap.serializeToString()) //numActive, curMaplen, key[i], value[i], ...
	return sb.String(), nil
}

func (s *LongSketch) toSlice() ([]byte, error) {
	emtpy := s.isEmpty()
	activeItems := s.getNumActiveItems()
	preLongs := 1
	outBytes := 8
	if !emtpy {
		preLongs = common.FamilyEnum.Frequency.MaxPreLongs //4
		outBytes = (preLongs + (2 * activeItems)) << 3     //2 because both keys and values are longs
	}
	outArr := make([]byte, outBytes)

	//build first preLong empty or not
	pre0 := int64(0)
	pre0 = insertPreLongs(int64(preLongs), pre0)                       //Byte 0
	pre0 = insertSerVer(_SER_VER, pre0)                                //Byte 1
	pre0 = insertFamilyID(int64(common.FamilyEnum.Frequency.Id), pre0) //Byte 2
	pre0 = insertLgMaxMapSize(int64(s.lgMaxMapSize), pre0)             //Byte 3
	pre0 = insertLgCurMapSize(int64(s.hashMap.lgLength), pre0)         //Byte 4
	if emtpy {
		pre0 = insertFlags(_EMPTY_FLAG_MASK, pre0) //Byte 5
		binary.LittleEndian.PutUint64(outArr, uint64(pre0))
		return outArr, nil
	}

	pre0 = insertFlags(0, pre0) //Byte 5
	preArr := make([]int64, preLongs)
	preArr[0] = pre0
	preArr[1] = insertActiveItems(int64(activeItems), pre0)
	preArr[2] = s.streamWeight
	preArr[3] = s.offset
	for i := 0; i < preLongs; i++ {
		binary.LittleEndian.PutUint64(outArr[i<<3:], uint64(preArr[i]))
	}
	//now the active items
	activeValues := s.hashMap.getActiveValues()
	activeKeys := s.hashMap.getActiveKeys()
	for i := 0; i < activeItems; i++ {
		binary.LittleEndian.PutUint64(outArr[(preLongs+i)<<3:], uint64(activeValues[i]))
		binary.LittleEndian.PutUint64(outArr[(preLongs+activeItems+i)<<3:], uint64(activeKeys[i]))
	}
	return outArr, nil
}

func (s *LongSketch) Reset() {
	hasMap, _ := NewReversePurgeLongHashMap(1 << _LG_MIN_MAP_SIZE)
	s.curMapCap = hasMap.getCapacity()
	s.offset = 0
	s.streamWeight = 0
	s.hashMap = hasMap
}

func (s *LongSketch) getFrequentItems(errorType ErrorType) ([]*Row, error) {
	return sortItems(s, s.getMaximumError(), errorType)
}
