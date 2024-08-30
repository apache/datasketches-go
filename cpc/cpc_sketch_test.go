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
	err = sk.UpdateCharSlice([]byte{5})
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
	assert.Equal(t, sk.GetFlavor(), CpcFlavorSparse)
	assert.Equal(t, sk.getFormat(), CpcFormatSparceHybridHip)
}

func TestCPCCheckEstimatesWithMerge(t *testing.T) {
	lgk := 4
	sk1, err := NewCpcSketch(lgk, CpcDefaultUpdateSeed)
	assert.NoError(t, err)
	sk2, err := NewCpcSketch(lgk, CpcDefaultUpdateSeed)
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

/*
  @Test
  public void checkEstimatesWithMerge() {
    final int lgK = 4;
    final CpcSketch sk1 = new CpcSketch(lgK);
    final CpcSketch sk2 = new CpcSketch(lgK);
    final int n = 1 << lgK;
    for (int i = 0; i < n; i++ ) {
      sk1.update(i);
      sk2.update(i + n);
    }
    final CpcUnion union = new CpcUnion(lgK);
    union.update(sk1);
    union.update(sk2);
    final CpcSketch result = union.getResult();
    final double est = result.getEstimate();
    final double lb = result.getLowerBound(2);
    final double ub = result.getUpperBound(2);
    assertTrue(lb >= 0);
    assertTrue(lb <= est);
    assertTrue(est <= ub);
    assertTrue(result.validate());
    println(result.toString(true));
  }
*/
