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
	"math"
)

// -----------------------------------------------------------------------------
// TestUtil Functions
// -----------------------------------------------------------------------------

// pwrLaw10NextDouble returns the next double in a power-law (base 10) sequence.
// ppb is "points per bin" and curPoint is the current value.
func pwrLaw10NextDouble(ppb int, curPoint float64) float64 {
	cur := curPoint
	if cur < 1.0 {
		cur = 1.0
	}
	// Generate the current index, rounded.
	gi := math.Round(math.Log10(cur) * float64(ppb))
	var next float64
	for {
		gi++ // increment the generating index
		next = math.Round(math.Pow(10.0, gi/float64(ppb)))
		if next > curPoint {
			break
		}
	}
	return next
}

// specialEquals compares two CpcSketch instances and panics via runtime assertions
// if they are not "special equal". It returns true if all assertions pass.
func specialEquals(sk1, sk2 *CpcSketch, sk1wasMerged, sk2wasMerged bool) bool {
	rtAssertEqualsUint64(sk1.seed, sk2.seed)
	rtAssertEqualsInt(sk1.lgK, sk2.lgK)
	rtAssertEqualsUint64(sk1.numCoupons, sk2.numCoupons)

	rtAssertEqualsInt(sk1.windowOffset, sk2.windowOffset)
	rtAssertEqualsBytes(sk1.slidingWindow, sk2.slidingWindow)
	pairTableEquals(sk1.pairTable, sk2.pairTable)

	ficolA := sk1.fiCol
	ficolB := sk2.fiCol

	if !sk1wasMerged && sk2wasMerged {
		rtAssert(!sk1.mergeFlag && sk2.mergeFlag)
		fiCol1 := calculateFirstInterestingColumn(sk1)
		rtAssertEqualsInt(fiCol1, sk2.fiCol)
	} else if sk1wasMerged && !sk2wasMerged {
		rtAssert(sk1.mergeFlag && !sk2.mergeFlag)
		fiCol2 := calculateFirstInterestingColumn(sk2)
		rtAssertEqualsInt(fiCol2, sk1.fiCol)
	} else {
		rtAssertEqualsBool(sk1.mergeFlag, sk2.mergeFlag)
		rtAssertEqualsInt(ficolA, ficolB)
		rtAssertEqualsFloat64(sk1.kxp, sk2.kxp, 0.01*sk1.kxp) // 1% tolerance
		rtAssertEqualsFloat64(sk1.hipEstAccum, sk2.hipEstAccum, 0.01*sk1.hipEstAccum)
	}
	return true
}

// calculateFirstInterestingColumn computes the "first interesting column" for the sketch.
// It iterates through the pair table slots and returns the minimum column index found,
// or the current window offset if none are lower.
func calculateFirstInterestingColumn(sk *CpcSketch) int {
	offset := sk.windowOffset
	if offset == 0 {
		return 0
	}
	table := sk.pairTable
	if table == nil {
		panic("pairTable is nil")
	}
	slots := table.slotsArr
	numSlots := 1 << table.lgSizeInts
	result := offset
	for i := 0; i < numSlots; i++ {
		rowCol := slots[i]
		if rowCol != -1 {
			col := rowCol & 63
			if col < result {
				result = col
			}
		}
	}
	return result
}

// -----------------------------------------------------------------------------
// Runtime Assertion Helpers (from RuntimeAsserts.java)
// These functions mimic Java asserts by panicking with an error message if
// the condition is not met.
// -----------------------------------------------------------------------------

// rtAssert panics if b is false.
func rtAssert(b bool) {
	if !b {
		panic("False, expected True.")
	}
}

// rtAssertFalse panics if b is true.
func rtAssertFalse(b bool) {
	if b {
		panic("True, expected False.")
	}
}

// rtAssertEqualsInt panics if a != b.
func rtAssertEqualsInt(a, b int) {
	if a != b {
		panic(fmt.Sprintf("%d != %d", a, b))
	}
}

// rtAssertEqualsUint64 panics if a != b.
func rtAssertEqualsUint64(a, b uint64) {
	if a != b {
		panic(fmt.Sprintf("%d != %d", a, b))
	}
}

// rtAssertEqualsFloat64 panics if the absolute difference between a and b is greater than eps.
func rtAssertEqualsFloat64(a, b, eps float64) {
	if math.Abs(a-b) > eps {
		panic(fmt.Sprintf("abs(%f - %f) > %f", a, b, eps))
	}
}

// rtAssertEqualsBool panics if a != b.
func rtAssertEqualsBool(a, b bool) {
	if a != b {
		panic(fmt.Sprintf("%v != %v", a, b))
	}
}

// rtAssertEqualsBytes panics if two byte slices are not equal.
func rtAssertEqualsBytes(a, b []byte) {
	if a == nil && b == nil {
		return
	}
	if a != nil && b != nil {
		if len(a) != len(b) {
			panic(fmt.Sprintf("Array lengths not equal: %d, %d", len(a), len(b)))
		}
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				panic(fmt.Sprintf("%d != %d at index %d", a[i], b[i], i))
			}
		}
		return
	}
	if a == nil {
		panic("Array a is nil")
	}
	panic("Array b is nil")
}

// -----------------------------------------------------------------------------
// PairTable Equality Check
// -----------------------------------------------------------------------------

// pairTableEquals compares two pair tables and panics if they are not equal.
func pairTableEquals(pt1, pt2 *pairTable) {
	if pt1 == nil && pt2 == nil {
		return
	}
	if pt1 == nil || pt2 == nil {
		panic("One of the pairTables is nil")
	}
	rtAssertEqualsInt(pt1.lgSizeInts, pt2.lgSizeInts)
	if len(pt1.slotsArr) != len(pt2.slotsArr) {
		panic(fmt.Sprintf("PairTable slot array lengths not equal: %d vs %d", len(pt1.slotsArr), len(pt2.slotsArr)))
	}
	for i := range pt1.slotsArr {
		if pt1.slotsArr[i] != pt2.slotsArr[i] {
			panic(fmt.Sprintf("PairTable slots differ at index %d: %d vs %d", i, pt1.slotsArr[i], pt2.slotsArr[i]))
		}
	}
}

// fieldError panics with an error indicating an illegal operation for the given format and hi-field.
func fieldError(format CpcFormat, hiField int) error {
	return fmt.Errorf("operation is illegal: Format = %s, HiField = %d", format.String(), hiField)
}
