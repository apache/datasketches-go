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

func TestCompositeEst(t *testing.T) {
	testComposite(t, 4, TgtHllType_HLL_4, 1000)
	testComposite(t, 5, TgtHllType_HLL_4, 1000)
	testComposite(t, 6, TgtHllType_HLL_4, 1000)
	testComposite(t, 13, TgtHllType_HLL_4, 10000)

	testComposite(t, 4, TgtHllType_HLL_6, 1000)
	testComposite(t, 5, TgtHllType_HLL_6, 1000)
	testComposite(t, 6, TgtHllType_HLL_6, 1000)
	testComposite(t, 13, TgtHllType_HLL_6, 10000)

	testComposite(t, 4, TgtHllType_HLL_8, 1000)
	testComposite(t, 5, TgtHllType_HLL_8, 1000)
	testComposite(t, 6, TgtHllType_HLL_8, 1000)
	testComposite(t, 13, TgtHllType_HLL_8, 10000)
}

func testComposite(t *testing.T, lgK int, tgtHllType TgtHllType, n int) {
	u, err := NewUnion(lgK)
	assert.NoError(t, err)
	sk, err := NewHllSketch(lgK, tgtHllType)
	assert.NoError(t, err)

	for i := 0; i < n; i++ {
		u.UpdateInt64(int64(i))
		sk.UpdateInt64(int64(i))
	}

	u.UpdateSketch(sk)
	res, err := u.GetResult(tgtHllType)
	assert.NoError(t, err)
	res.GetCompositeEstimate()
}

func TestBigHipGetRse(t *testing.T) {
	sk, err := NewHllSketch(13, TgtHllType_HLL_8)
	assert.NoError(t, err)

	for i := 0; i < 10000; i++ {
		sk.UpdateInt64(int64(i))
	}
}

func TestToArraySliceDeserialize(t *testing.T) {
	lgK := 4
	u := 8
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_4, u)
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_6, u)
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_8, u)

	lgK = 16
	u = (((1 << (lgK - 3)) * 3) / 4) + 100
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_4, u)
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_6, u)
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_8, u)

	lgK = 21
	u = (((1 << (lgK - 3)) * 3) / 4) + 1000
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_4, u)
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_6, u)
	toArraySliceDeserialize(t, lgK, TgtHllType_HLL_8, u)
}

func toArraySliceDeserialize(t *testing.T, lgK int, tgtHllType TgtHllType, u int) {
	sk1, err := NewHllSketch(lgK, tgtHllType)
	assert.NoError(t, err)

	for i := 0; i < u; i++ {
		sk1.UpdateInt64(int64(i))
	}
	_, isArray := sk1.(*hllSketchImpl).sketch.(hllArray)
	assert.True(t, isArray)

	// Update
	est1 := sk1.GetEstimate()
	assert.InDelta(t, est1, u, float64(u)*.03)
	est := sk1.GetHipEstimate()
	assert.Equal(t, est, est1, 0.0)

	// misc
	sk1.(*hllSketchImpl).putRebuildCurMinNumKxQFlag(true)
	sk1.(*hllSketchImpl).putRebuildCurMinNumKxQFlag(false)

	sl1, err := sk1.ToCompactSlice()
	assert.NoError(t, err)
	sk2, e := DeserializeHllSketch(sl1, true)
	assert.NoError(t, e)
	est2 := sk2.GetEstimate()
	assert.Equal(t, est2, est1, 0.0)

	err = sk1.Reset()
	assert.NoError(t, err)
	est = sk1.GetEstimate()
	assert.Equal(t, est, 0.0, 0.0)
}
