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
	lgMaxMapSize = max(lgMaxMapSize, lgMinMapSize)
	lgCurMapSize = max(lgCurMapSize, lgMinMapSize)
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

// NewLongSketchWithDefault constructs a new LongSketch with the given maxMapSize and the
// default initialMapSize (8).
// maxMapSize determines the physical size of the internal hash map managed by this
// sketch and must be a power of 2.  The maximum capacity of this internal hash map is
// 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a
// function of maxMapSize.
func NewLongSketchWithDefault(maxMapSize int) (*LongSketch, error) {
	log2OfInt, err := common.ExactLog2(maxMapSize)
	if err != nil {
		return nil, fmt.Errorf("maxMapSize, %e", err)
	}
	return NewLongSketch(log2OfInt, lgMinMapSize)
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
	if serVe != serVer {
		return nil, fmt.Errorf("Possible Corruption: Bad SerVer: %d", serVe)
	}
	if famID != common.FamilyFrequencyId {
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

func (s *LongSketch) getNumActiveItems() int {
	return s.hashMap.numActive
}

// getMaximumMapCapacity returns the maximum number of counters the sketch is configured to
// support.
func (s *LongSketch) getMaximumMapCapacity() int {
	return int(float64(uint64(1<<s.lgMaxMapSize)) * loadFactor)
}

func (s *LongSketch) getCurrentMapCapacity() int {
	return s.curMapCap
}

func (s *LongSketch) getStreamLength() int64 {
	return s.streamWeight
}

func (s *LongSketch) isEmpty() bool {
	return s.getNumActiveItems() == 0
}

func (s *LongSketch) Update(item int64, count int64) error {
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
		err := s.Update(iter.getKey(), iter.getValue())
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
	serVer := serVer //0
	famID := common.FamilyFrequencyId
	lgMaxMapSz := s.lgMaxMapSize
	flags := 0
	if s.hashMap.numActive == 0 {
		flags = emptyFlagMask
	}
	_, err := fmt.Fprintf(&sb, "%d,%d,%d,%d,%d,%d,", serVer, famID, lgMaxMapSz, flags, s.streamWeight, s.offset)
	if err != nil {
		return "", err
	}
	sb.WriteString(s.hashMap.serializeToString()) //numActive, curMaplen, key[i], value[i], ...
	return sb.String(), nil
}
