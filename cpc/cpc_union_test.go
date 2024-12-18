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

func TestCpcUnionError(t *testing.T) {
	sk, err := NewCpcSketch(10, 1)
	union, err := NewCpcUnionSketchWithDefault(defaultLgK)
	err = union.Update(sk)
	if err == nil {
		t.Errorf("Expected error")
	}
}

func TestCpcGetters(t *testing.T) {
	lgK := 10
	union, err := NewCpcUnionSketchWithDefault(lgK)
	if err != nil {
		t.Errorf("Error: %s", err)
	}
	assert.Equal(t, union.lgK, lgK)
	assert.Equal(t, union.getNumCoupons(), uint64(0))
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	for i := 0; i <= (4 << lgK); i++ {
		err := sk.UpdateInt64(int64(i))
		assert.NoError(t, err)
	}
	err = union.Update(sk)
	assert.NoError(t, err)
	assert.True(t, union.getNumCoupons() > 0)
	assert.NotNil(t, getBitMatrix(union))
	assert.Equal(t, union.GetFamilyId(), internal.FamilyEnum.CPC.Id)

}

func getBitMatrix(union CpcUnion) []uint64 {
	//checkUnionState(union)
	if union.bitMatrix != nil {
		return union.bitMatrix
	}
	return bitMatrixOfSketch(union.accumulator)
}
