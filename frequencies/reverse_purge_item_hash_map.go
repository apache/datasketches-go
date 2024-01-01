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
	"github.com/apache/datasketches-go/internal"
)

type reversePurgeItemHashMap[C comparable] struct {
	lgLength      int
	loadThreshold int
	keys          []C
	values        []int64
	states        []int16
	numActive     int
	hasher        ItemSketchHasher[C]
}

const (
	reversePurgeItemHashMapLoadFactor = float64(0.75)
)

// newReversePurgeItemHashMap will create arrays of length mapSize, which must be a power of two.
// This restriction was made to ensure fast hashing.
// The variable this.loadThreshold is then set to the largest value that
// will not overload the hashFn table.
//
//   - mapSize, This determines the number of cells in the arrays underlying the
//     HashMap implementation and must be a power of 2.
//     The hashFn table will be expected to store reversePurgeItemHashMapLoadFactor * mapSize (key, value) pairs.
func newReversePurgeItemHashMap[C comparable](mapSize int, hasher ItemSketchHasher[C]) (*reversePurgeItemHashMap[C], error) {
	lgLength, err := internal.ExactLog2(mapSize)
	if err != nil {
		return nil, err
	}
	return &reversePurgeItemHashMap[C]{
		lgLength,
		int(float64(mapSize) * reversePurgeItemHashMapLoadFactor),
		make([]C, mapSize),
		make([]int64, mapSize),
		make([]int16, mapSize),
		0,
		hasher,
	}, nil
}

func (r *reversePurgeItemHashMap[C]) getCapacity() int {
	return r.loadThreshold
}

func (r *reversePurgeItemHashMap[C]) get(key C) (int64, error) {
	//if key == nil {
	//	return 0, nil
	//}
	probe := r.hashProbe(key)
	if r.states[probe] > 0 {
		if r.keys[probe] != key {
			return 0, fmt.Errorf("key not found")
		}
		return r.values[probe], nil

	}
	return 0, nil
}

func (r *reversePurgeItemHashMap[C]) hashProbe(key C) int {
	arrayMask := uint64(len(r.keys) - 1)

	probe := r.hasher.Hash(key) & arrayMask
	for r.states[probe] > 0 && r.keys[probe] != key {
		probe = probe + 1&arrayMask
	}
	return int(probe)
}
