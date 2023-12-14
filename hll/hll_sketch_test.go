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
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	javaPath = "../serialization_test_data/java_generated_files"
	cppPath  = "../serialization_test_data/cpp_generated_files"
	goPath   = "../serialization_test_data/go_generated_files"
)

func TestMisc(t *testing.T) {
	hll, err := NewHllSketch(10, TgtHllType_HLL_4)
	assert.NoError(t, err)
	assert.True(t, hll.IsEstimationMode())
	err = hll.Reset()
	assert.NoError(t, err)
	assert.Equal(t, serVer, hll.GetSerializationVersion())
}

func TestUpdateTypes(t *testing.T) {
	checkUpdateType(t, TgtHllType_HLL_4)
	checkUpdateType(t, TgtHllType_HLL_6)
	checkUpdateType(t, TgtHllType_HLL_8)
}

func checkUpdateType(t *testing.T, tgtHllType TgtHllType) {
	hll, err := NewHllSketch(11, tgtHllType)
	assert.NoError(t, err)

	hll.UpdateSlice(nil)
	hll.UpdateSlice(make([]byte, 0))
	hll.UpdateSlice([]byte{1, 2, 3})
	hll.UpdateString("")
	hll.UpdateString("abc")

	hll.UpdateInt64(0)
	hll.UpdateInt64(1)
	hll.UpdateInt64(-1)

	hll.UpdateUInt64(0)
	hll.UpdateUInt64(1)
}

func TestCopies(t *testing.T) {
	checkCopy(t, 14, TgtHllType_HLL_4)
	checkCopy(t, 8, TgtHllType_HLL_6)
	checkCopy(t, 8, TgtHllType_HLL_8)
}

func checkCopy(t *testing.T, lgK int, tgtHllType TgtHllType) {
	sk, err := NewHllSketch(lgK, tgtHllType)
	assert.NoError(t, err)
	for i := 0; i < 7; i++ {
		sk.UpdateInt64(int64(i))
	}
	assert.Equal(t, curMode_LIST, sk.GetCurMode())

	skCopy := sk.Copy()
	assert.Equal(t, curMode_LIST, skCopy.GetCurMode())

	impl1 := sk.(*hllSketchImpl).sketch
	impl2 := skCopy.(*hllSketchImpl).sketch

	assert.Equal(t, impl1.(*couponListImpl).couponCount, impl2.(*couponListImpl).couponCount)

	est1 := impl1.GetEstimate()
	est2 := impl2.GetEstimate()
	assert.Equal(t, est1, est2)
	assert.False(t, impl1 == impl2)

	for i := 7; i < 24; i++ {
		sk.UpdateInt64(int64(i))
	}

	assert.Equal(t, curMode_SET, sk.GetCurMode())
	skCopy = sk.Copy()
	assert.Equal(t, curMode_SET, skCopy.GetCurMode())

	impl1 = sk.(*hllSketchImpl).sketch
	impl2 = skCopy.(*hllSketchImpl).sketch

	assert.Equal(t, impl1.(*couponHashSetImpl).couponCount, impl2.(*couponHashSetImpl).couponCount)
	est1 = impl1.GetEstimate()
	est2 = impl2.GetEstimate()
	assert.Equal(t, est1, est2)
	assert.False(t, impl1 == impl2)

	u := 25
	if tgtHllType == TgtHllType_HLL_4 {
		u = 100000
	}
	for i := 24; i < u; i++ {
		sk.UpdateInt64(int64(i))
	}

	assert.Equal(t, curMode_HLL, sk.GetCurMode())
	skCopy = sk.Copy()
	assert.Equal(t, curMode_HLL, skCopy.GetCurMode())

	impl1 = sk.(*hllSketchImpl).sketch
	impl2 = skCopy.(*hllSketchImpl).sketch

	est1 = impl1.GetEstimate()
	est2 = impl2.GetEstimate()
	assert.Equal(t, est1, est2)
	assert.False(t, impl1 == impl2)
}

func TestCopyAs(t *testing.T) {
	checkCopyAs(t, TgtHllType_HLL_4, TgtHllType_HLL_4)
	checkCopyAs(t, TgtHllType_HLL_4, TgtHllType_HLL_6)
	checkCopyAs(t, TgtHllType_HLL_4, TgtHllType_HLL_8)
	checkCopyAs(t, TgtHllType_HLL_6, TgtHllType_HLL_4)
	checkCopyAs(t, TgtHllType_HLL_6, TgtHllType_HLL_6)
	checkCopyAs(t, TgtHllType_HLL_6, TgtHllType_HLL_8)
	checkCopyAs(t, TgtHllType_HLL_8, TgtHllType_HLL_4)
	checkCopyAs(t, TgtHllType_HLL_8, TgtHllType_HLL_6)
	checkCopyAs(t, TgtHllType_HLL_8, TgtHllType_HLL_8)
}

func checkCopyAs(t *testing.T, srcType TgtHllType, dstType TgtHllType) {
	var (
		lgK  = 8
		n1   = 7
		n2   = 24
		n3   = 1000
		base = 0
	)

	src, err := NewHllSketch(lgK, srcType)
	assert.NoError(t, err)
	for i := 0; i < n1; i++ {
		src.UpdateInt64(int64(i + base))
	}
	dst := src.CopyAs(dstType)
	srcEst := src.GetEstimate()
	dstEst := dst.GetEstimate()
	assert.Equal(t, srcEst, dstEst)

	for i := n1; i < n2; i++ {
		src.UpdateInt64(int64(i + base))
	}
	dst = src.CopyAs(dstType)
	srcEst = src.GetEstimate()
	dstEst = dst.GetEstimate()
	assert.Equal(t, srcEst, dstEst)

	for i := n2; i < n3; i++ {
		src.UpdateInt64(int64(i + base))
	}
	dst = src.CopyAs(dstType)
	srcEst = src.GetEstimate()
	dstEst = dst.GetEstimate()
	assert.Equal(t, srcEst, dstEst)
}

func TestNewHLLDataSketchUint(t *testing.T) {
	tgts := []TgtHllType{TgtHllType_HLL_4, TgtHllType_HLL_6, TgtHllType_HLL_8}
	ns := []int{1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, tgt := range tgts {
		hll, err := NewHllSketch(11, tgt)
		assert.NoError(t, err)
		for _, n := range ns {
			for i := 0; i < n; i++ {
				hll.UpdateUInt64(uint64(i))
			}
			est := hll.GetEstimate()
			assert.InDelta(t, n, est, float64(n)*0.03)
		}
	}
}

func TestNewHLLDataSketchString(t *testing.T) {
	tgts := []TgtHllType{TgtHllType_HLL_4, TgtHllType_HLL_6, TgtHllType_HLL_8}
	ns := []int{1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, tgt := range tgts {
		hll, err := NewHllSketch(11, tgt)
		assert.NoError(t, err)
		for _, n := range ns {
			for i := 0; i < n; i++ {
				hll.UpdateString(strconv.Itoa(i))
			}
			est := hll.GetEstimate()
			assert.InDelta(t, n, est, float64(n)*0.03)
		}
	}
}

func TestHLLDataSketchT(b *testing.T) {
	hll, err := NewHllSketch(21, TgtHllType_HLL_4)
	assert.NoError(b, err)
	for i := 0; i < 1000000; i++ {
		hll.UpdateUInt64(uint64(i))
	}
	est := hll.GetEstimate()
	assert.InDelta(b, 1000000, est, float64(1000000)*0.03)

}

func BenchmarkHLLDataSketch(b *testing.B) {
	// HLL uint64 BenchMark
	b.Run("lgK4 HLL4 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(4, TgtHllType_HLL_4)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})
	b.Run("lgK16 HLL4 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllType_HLL_4)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})
	b.Run("lgK21 HLL4 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllType_HLL_4)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})

	b.Run("lgK4 HLL6 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllType_HLL_6)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})
	b.Run("lgK16 HLL6 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllType_HLL_6)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})
	b.Run("lgK21 HLL6 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllType_HLL_6)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})

	b.Run("lgK4 HLL8 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllType_HLL_8)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})
	b.Run("lgK16 HLL8 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllType_HLL_8)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})
	b.Run("lgK21 HLL8 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllType_HLL_8)
		for i := 0; i < b.N; i++ {
			hll.UpdateUInt64(uint64(i))
		}
	})

	// HLL Slice BenchMark
	bs := make([]byte, 8)
	b.Run("lgK4 HLL4 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllType_HLL_4)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})
	b.Run("lgK16 HLL4 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllType_HLL_4)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})
	b.Run("lgK21 HLL4 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllType_HLL_4)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})

	b.Run("lgK4 HLL6 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllType_HLL_6)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})
	b.Run("lgK16 HLL6 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllType_HLL_6)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})
	b.Run("lgK21 HLL6 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllType_HLL_6)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})

	b.Run("lgK4 HLL8 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllType_HLL_8)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})
	b.Run("lgK16 HLL8 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllType_HLL_8)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})
	b.Run("lgK21 HLL8 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllType_HLL_8)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(bs, uint64(i))
			hll.UpdateSlice(bs)
		}
	})

	// Union benchmark
	b.Run("lgK4 HLL8 union", func(b *testing.B) {
		hll, _ := NewHllSketch(4, TgtHllType_HLL_8)
		union, _ := NewUnion(4)
		for i := 0; i < b.N; i++ {
			hll.UpdateSlice(bs)
			union.UpdateSketch(hll)
		}
	})
	b.Run("lgK16 HLL8 union", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllType_HLL_8)
		union, _ := NewUnion(16)
		for i := 0; i < b.N; i++ {
			hll.UpdateSlice(bs)
			union.UpdateSketch(hll)
		}
	})
	b.Run("lgK21 HLL8 union", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllType_HLL_8)
		union, _ := NewUnion(21)
		for i := 0; i < b.N; i++ {
			hll.UpdateSlice(bs)
			union.UpdateSketch(hll)
		}
	})

}

func BenchmarkHLLDataSketchWithEstimate(b *testing.B) {
	hll, err := NewHllSketch(11, TgtHllType_HLL_8)
	assert.NoError(b, err)
	for i := 0; i < b.N; i++ {
		hll.UpdateString(strconv.Itoa(i))
	}
	est := hll.GetEstimate()

	estimate := int64(est)
	fmt.Printf("Estimated cardinality: %d (true: %d) (error: %f)\n ", estimate, b.N, float64(int64(b.N)-estimate)*100/float64(b.N))
}

// Test the hard case for (shiftedNewValue >= AUX_TOKEN) && (rawStoredOldNibble = AUX_TOKEN)
func TestHLL4RawStoredOldNibbleAndShiftedNewValueAuxToken(t *testing.T) {
	hll, _ := NewHllSketch(21, TgtHllType_HLL_4)
	for i := uint64(0); i < 29197004; i++ {
		hll.UpdateUInt64(i)
	}
	hll.UpdateUInt64(29197004)
}
