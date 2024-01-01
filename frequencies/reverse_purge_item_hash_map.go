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
	"math/bits"
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
	if isNil(key) {
		return 0, nil
	}

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

func (r *reversePurgeItemHashMap[C]) adjustOrPutValue(key C, adjustAmount int64) error {
	arrayMask := len(r.keys) - 1
	probe := r.hashProbe(key) & arrayMask
	drift := 1
	for r.states[probe] != 0 && r.keys[probe] != key {
		probe = (probe + 1) & arrayMask
		drift++
		//only used for theoretical analysis
		//assert drift < DRIFT_LIMIT : "drift: " + drift + " >= DRIFT_LIMIT";
	}

	if r.states[probe] == 0 {
		// adding the key to the table the value
		if r.numActive >= r.loadThreshold {
			return fmt.Errorf("numActive: %d >= loadThreshold: %d", r.numActive, r.loadThreshold)
		}
		r.keys[probe] = key
		r.values[probe] = adjustAmount
		r.states[probe] = int16(drift)
		r.numActive++
	} else {
		// adjusting the value of an existing key
		if r.keys[probe] != key {
			return fmt.Errorf("key not found")
		}
		r.values[probe] += adjustAmount
	}
	return nil
}

func (r *reversePurgeItemHashMap[C]) resize(newSize int) error {
	oldKeys := r.keys
	oldValues := r.values
	oldStates := r.states
	r.keys = make([]C, newSize)
	r.values = make([]int64, newSize)
	r.states = make([]int16, newSize)
	r.loadThreshold = int(float64(newSize) * reversePurgeItemHashMapLoadFactor)
	r.lgLength = bits.TrailingZeros64(uint64(newSize))
	r.numActive = 0
	for i := 0; i < len(oldKeys); i++ {
		if oldStates[i] > 0 {
			err := r.adjustOrPutValue(oldKeys[i], oldValues[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *reversePurgeItemHashMap[C]) isActive(probe int) bool {
	return r.states[probe] > 0
}

func (r *reversePurgeItemHashMap[C]) purge(sampleSize int) int64 {
	limit := min(sampleSize, r.numActive)
	numSamples := 0
	i := 0
	samples := make([]int64, limit)
	for numSamples < limit {
		if r.isActive(i) {
			samples[numSamples] = r.values[i]
			numSamples++
		}
		i++
	}
	val := internal.QuickSelect(samples, 0, numSamples-1, limit/2)
	r.adjustAllValuesBy(-1 * val)
	r.keepOnlyPositiveCounts()
	return val
}

func (r *reversePurgeItemHashMap[C]) adjustAllValuesBy(adjustAmount int64) {
	for i := len(r.values); i > 0; {
		i--
		r.values[i] += adjustAmount
	}
}

func (r *reversePurgeItemHashMap[C]) keepOnlyPositiveCounts() {
	// Starting from the back, find the first empty cell,
	//  which establishes the high end of a cluster.
	firstProbe := len(r.states) - 1
	for r.states[firstProbe] > 0 {
		firstProbe--
	}
	// firstProbe keeps track of this point.
	// When we find the next non-empty cell, we know we are at the high end of a cluster
	// Work towards the front; delete any non-positive entries.
	for probe := firstProbe; probe > 0; {
		probe--
		if r.states[probe] > 0 && r.values[probe] <= 0 {
			r.hashDelete(probe) //does the work of deletion and moving higher items towards the front.
			r.numActive--
		}
	}
	// now work on the first cluster that was skipped.
	for probe := len(r.states); probe > firstProbe; {
		probe--
		if r.states[probe] > 0 && r.values[probe] <= 0 {
			r.hashDelete(probe)
			r.numActive--
		}
	}
}

func (r *reversePurgeItemHashMap[C]) hashDelete(deleteProbe int) {
	// Looks ahead in the table to search for another
	// item to move to this location
	// if none are found, the status is changed
	r.states[deleteProbe] = 0 //mark as empty
	drift := 1
	arrayMask := len(r.keys) - 1
	probe := (deleteProbe + drift) & arrayMask //map length must be a power of 2
	// advance until you find a free location replacing locations as needed
	for r.states[probe] != 0 {
		if r.states[probe] > int16(drift) {
			// move current element
			r.keys[deleteProbe] = r.keys[probe]
			r.values[deleteProbe] = r.values[probe]
			r.states[deleteProbe] = int16(r.states[probe] - int16(drift))
			// marking this location as deleted
			r.states[probe] = 0
			drift = 0
			deleteProbe = probe
		}
		probe = (probe + 1) & arrayMask
		drift++
		//only used for theoretical analysis
		//assert drift < DRIFT_LIMIT : "drift: " + drift + " >= DRIFT_LIMIT";
	}
}
