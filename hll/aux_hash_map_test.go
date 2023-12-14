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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMustReplace(t *testing.T) {
	auxMap := newAuxHashMap(3, 7)
	auxMap.mustAdd(100, 5)
	val := auxMap.mustFindValueFor(100)
	assert.Equal(t, 5, val)
	auxMap.mustReplace(100, 10)
	val = auxMap.mustFindValueFor(100)
	assert.Equal(t, 10, val)
	assert.Panics(t, func() { auxMap.mustReplace(101, 5) }, "pair not found: SlotNo: 101, Value: 5")
}

func TestGrowAuxSpace(t *testing.T) {
	auxMap := newAuxHashMap(3, 7)
	assert.Equal(t, 3, auxMap.getLgAuxArrInts())
	for i := 1; i <= 7; i++ {
		auxMap.mustAdd(i, i)
	}
	assert.Equal(t, 4, auxMap.getLgAuxArrInts())
	itr := auxMap.iterator()

	var (
		count1 = 0
		count2 = 0
	)

	for itr.nextAll() {
		count2++
		pair := itr.getPair()
		if pair != 0 {
			count1++
		}
	}
	assert.Equal(t, 7, count1)
	assert.Equal(t, 16, count2)
}

func TestExceptions1(t *testing.T) {
	auxMap := newAuxHashMap(3, 7)
	auxMap.mustAdd(100, 5)
	assert.Panics(t, func() { auxMap.mustFindValueFor(101) }, "SlotNo not found: 101")
}

func TestExceptions2(t *testing.T) {
	auxMap := newAuxHashMap(3, 7)
	auxMap.mustAdd(100, 5)
	assert.Panics(t, func() { auxMap.mustAdd(100, 6) }, "found a slotNo that should not be there: SlotNo: 100, Value: 6")
}
