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
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCPCCheckUpdatesEstimate(t *testing.T) {
	sk, err := NewCpcSketch(10, 0)
	assert.NoError(t, err)
	assert.Equal(t, sk.getFormat(), CpcFormatEmptyHip)
	err = sk.UpdateUint64(1)
	assert.NoError(t, err)
	err = sk.UpdateFloat64(2.0)
	assert.NoError(t, err)
	err = sk.UpdateString("3")
	assert.NoError(t, err)
	bytes := []byte{4, 4}
	err = sk.UpdateByteSlice(bytes)
	assert.NoError(t, err)
	bytes2 := []byte{4}
	err = sk.UpdateByteSlice(bytes2)
	assert.NoError(t, err)
	err = sk.UpdateByteSlice([]byte{5})
	assert.NoError(t, err)
	err = sk.UpdateInt32Slice([]int32{6})
	assert.NoError(t, err)
	err = sk.UpdateInt64Slice([]int64{7})
	assert.NoError(t, err)
	est := sk.GetEstimate()
	lb := sk.GetLowerBound(2)
	ub := sk.GetUpperBound(2)
	assert.True(t, lb >= 0)
	assert.True(t, lb <= est)
	assert.True(t, est <= ub)
	assert.Equal(t, sk.getFlavor(), CpcFlavorSparse)
	assert.Equal(t, sk.getFormat(), CpcFormatSparceHybridHip)
}

func TestCPCCheckEstimatesWithMerge(t *testing.T) {
	lgk := 4
	sk1, err := NewCpcSketch(lgk, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	sk2, err := NewCpcSketch(lgk, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	n := 1 << lgk
	for i := 0; i < n; i++ {
		err = sk1.UpdateUint64(uint64(i))
		assert.NoError(t, err)
		err = sk2.UpdateUint64(uint64(i + n))
		assert.NoError(t, err)
	}
	union, err := NewCpcUnionSketchWithDefault(lgk)
	assert.NoError(t, err)
	err = union.Update(sk1)
	assert.NoError(t, err)
	err = union.Update(sk2)
	assert.NoError(t, err)
	result, err := union.GetResult()
	assert.NoError(t, err)
	est := result.GetEstimate()
	lb := result.GetLowerBound(2)
	ub := result.GetUpperBound(2)
	assert.True(t, lb >= 0)
	assert.True(t, lb <= est)
	assert.True(t, est <= ub)
}

func TestCPCCheckCornerCaseUpdates(t *testing.T) {
	lgK := 4
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	err = sk.UpdateFloat64(0.0)
	assert.NoError(t, err)
	err = sk.UpdateFloat64(-0.0)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))
	err = sk.UpdateString("")
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))

	err = sk.UpdateByteSlice(nil)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))
	emptySlice := make([]byte, 0)
	err = sk.UpdateByteSlice(emptySlice)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))

	err = sk.UpdateInt32Slice(nil)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))
	emptyInt32Slice := make([]int32, 0)
	err = sk.UpdateInt32Slice(emptyInt32Slice)
	assert.NoError(t, err)

	err = sk.UpdateInt64Slice(nil)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))
	emptyInt64Slice := make([]int64, 0)
	err = sk.UpdateInt64Slice(emptyInt64Slice)
	assert.NoError(t, err)
}

func TestCPCCheckCornerHashUpdates(t *testing.T) {
	sk, err := NewCpcSketch(26, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	// In Java, hash0 is -1; in Go we represent -1 as all ones.
	hash0 := ^uint64(0)
	hash1 := uint64(0)
	err = sk.hashUpdate(hash0, hash1)
	assert.NoError(t, err)
	assert.NotNil(t, sk.pairTable)
}

// TestCPCCheckCopyWithWindow tests the copy() method and then refreshes KXP.
func TestCPCCheckCopyWithWindow(t *testing.T) {
	lgK := 4
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	sk2, err := sk.copy()
	assert.NoError(t, err)
	n := 1 << lgK
	for i := 0; i < n; i++ {
		err = sk.UpdateUint64(uint64(i))
		assert.NoError(t, err)
	}
	sk2, err = sk.copy()
	assert.NoError(t, err)
	bitMatrix, err := sk.bitMatrixOfSketch()
	assert.NoError(t, err)
	sk.refreshKXP(bitMatrix)
	assert.True(t, specialEquals(sk2, sk, false, false))
}

// TestCPCCheckFamily verifies that GetFamily returns the CPC family enum.
func TestCPCCheckFamily(t *testing.T) {
	sk, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)

	family := sk.getFamily()

	assert.Equal(t, family, internal.FamilyEnum.CPC.Id)
}

func TestCPCCheckLgK(t *testing.T) {
	sk, err := NewCpcSketch(10, 0)
	assert.NoError(t, err)
	assert.Equal(t, sk.lgK, 10)
	_, err = NewCpcSketch(3, 0)
	assert.Error(t, err)
	sk, err = NewCpcSketchWithDefault(defaultLgK)
	assert.NoError(t, err)
	assert.Equal(t, sk.lgK, defaultLgK)
	assert.Equal(t, sk.seed, internal.DEFAULT_UPDATE_SEED)
}

func TestCPCcheckIconHipUBLBLg15(t *testing.T) {
	iconConfidenceUB(15, 1, 2)
	iconConfidenceLB(15, 1, 2)
	hipConfidenceUB(15, 1, 1.0, 2)
	hipConfidenceLB(15, 1, 1.0, 2)
}

// TestCPCCheckRowColUpdate tests that rowColUpdate properly updates the sketch.
func TestCPCCheckRowColUpdate(t *testing.T) {
	lgK := 10
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	err = sk.rowColUpdate(0)
	assert.NoError(t, err)
	assert.Equal(t, CpcFlavorSparse, sk.getFlavor())
}

// TestCPCCheckGetMaxSize verifies the maximum serialized size calculations.
func TestCPCCheckGetMaxSize(t *testing.T) {
	size4, err := getMaxSerializedBytes(4)
	assert.NoError(t, err)
	size26, err := getMaxSerializedBytes(26)
	assert.NoError(t, err)
	assert.Equal(t, 24+40, size4)

	expectedFloat := 0.6 * float64(1<<26)
	expected := int(expectedFloat) + 40
	assert.Equal(t, expected, size26)
}
