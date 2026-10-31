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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/internal/testutil"
)

func TestGenerateGoSnapshots(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range nArr {
		hll4, err := NewHllSketch(defaultLgK, TgtHllTypeHll4)
		assert.NoError(t, err)
		hll6, err := NewHllSketch(defaultLgK, TgtHllTypeHll6)
		assert.NoError(t, err)
		hll8, err := NewHllSketch(defaultLgK, TgtHllTypeHll8)
		assert.NoError(t, err)

		for i := 0; i < n; i++ {
			assert.NoError(t, hll4.UpdateUInt64(uint64(i)))
			assert.NoError(t, hll6.UpdateUInt64(uint64(i)))
			assert.NoError(t, hll8.UpdateUInt64(uint64(i)))
		}
		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)

		sl4, err := hll4.ToCompactSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll4_n%d_go.sk", internal.GoPath, n), sl4, 0644)
		assert.NoError(t, err)

		sl6, err := hll6.ToCompactSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll6_n%d_go.sk", internal.GoPath, n), sl6, 0644)
		assert.NoError(t, err)

		sl8, err := hll8.ToCompactSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll8_n%d_go.sk", internal.GoPath, n), sl8, 0644)
		assert.NoError(t, err)
	}
}

func TestJavaCompat(t *testing.T) {
	t.Run("Java Hll4", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			filename := fmt.Sprintf("%s/hll4_n%d_java.sk", internal.JavaPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			sketch, err := NewHllSketchFromSlice(bytes, true)
			if err != nil {
				return
			}

			assert.Equal(t, 12, sketch.GetLgConfigK())
			est, err := sketch.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.02)
		}
	})

	t.Run("Java Hll6", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			filename := fmt.Sprintf("%s/hll6_n%d_java.sk", internal.JavaPath, n)
			bytes := testutil.ReadSnapshot(t, filename)

			sketch, err := NewHllSketchFromSlice(bytes, true)
			if err != nil {
				return
			}

			assert.Equal(t, 12, sketch.GetLgConfigK())
			est, err := sketch.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.02)
		}
	})

	t.Run("Java Hll8", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			filename := fmt.Sprintf("%s/hll8_n%d_java.sk", internal.JavaPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			sketch, err := NewHllSketchFromSlice(bytes, true)
			if err != nil {
				return
			}

			assert.Equal(t, 12, sketch.GetLgConfigK())
			est, err := sketch.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.02)
		}
	})
}

func TestCppCompat(t *testing.T) {
	t.Run("Cpp Hll4", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			filename := fmt.Sprintf("%s/hll4_n%d_cpp.sk", internal.CppPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			sketch, err := NewHllSketchFromSlice(bytes, true)
			if err != nil {
				return
			}

			assert.Equal(t, 12, sketch.GetLgConfigK())
			est, err := sketch.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.02)
		}
	})

	t.Run("Cpp Hll6", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			filename := fmt.Sprintf("%s/hll6_n%d_cpp.sk", internal.CppPath, n)
			bytes := testutil.ReadSnapshot(t, filename)

			sketch, err := NewHllSketchFromSlice(bytes, true)
			if err != nil {
				return
			}

			assert.Equal(t, 12, sketch.GetLgConfigK())
			est, err := sketch.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.02)
		}
	})

	t.Run("Cpp Hll8", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			filename := fmt.Sprintf("%s/hll8_n%d_cpp.sk", internal.CppPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			sketch, err := NewHllSketchFromSlice(bytes, true)
			if err != nil {
				return
			}

			assert.Equal(t, 12, sketch.GetLgConfigK())
			est, err := sketch.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.02)
		}
	})
}

func TestGoCompat(t *testing.T) {
	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range nArr {
		hll4, err := NewHllSketch(defaultLgK, TgtHllTypeHll4)
		assert.NoError(t, err)
		hll6, err := NewHllSketch(defaultLgK, TgtHllTypeHll6)
		assert.NoError(t, err)
		hll8, err := NewHllSketch(defaultLgK, TgtHllTypeHll8)
		assert.NoError(t, err)

		for i := 0; i < n; i++ {
			assert.NoError(t, hll4.UpdateUInt64(uint64(i)))
			assert.NoError(t, hll6.UpdateUInt64(uint64(i)))
			assert.NoError(t, hll8.UpdateUInt64(uint64(i)))
		}

		{
			sl4, err := hll4.ToCompactSlice()
			assert.NoError(t, err)
			filename := fmt.Sprintf("%s/hll4_n%d_java.sk", internal.JavaPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			assert.Equal(t, bytes, sl4)
		}

		{
			sl6, err := hll6.ToCompactSlice()
			assert.NoError(t, err)
			filename := fmt.Sprintf("%s/hll6_n%d_java.sk", internal.JavaPath, n)
			bytes := testutil.ReadSnapshot(t, filename)

			if extractCurMode(bytes) == curModeHll {
				bytes[5] = setCompactFlag(bytes[5])
			}
			assert.Equal(t, bytes, sl6)
		}

		{
			sl8, err := hll8.ToCompactSlice()
			assert.NoError(t, err)
			filename := fmt.Sprintf("%s/hll8_n%d_java.sk", internal.JavaPath, n)
			bytes := testutil.ReadSnapshot(t, filename)

			if extractCurMode(bytes) == curModeHll {
				bytes[5] = setCompactFlag(bytes[5])
			}
			assert.Equal(t, bytes, sl8)
		}

		{
			sl4, err := hll4.ToCompactSlice()
			assert.NoError(t, err)
			filename := fmt.Sprintf("%s/hll4_n%d_cpp.sk", internal.CppPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			assert.Equal(t, bytes, sl4)
		}

		{
			sl6, err := hll6.ToCompactSlice()
			assert.NoError(t, err)
			filename := fmt.Sprintf("%s/hll6_n%d_cpp.sk", internal.CppPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			assert.Equal(t, bytes, sl6, "n: %d", n)
		}

		{
			sl8, err := hll8.ToCompactSlice()
			assert.NoError(t, err)
			filename := fmt.Sprintf("%s/hll8_n%d_cpp.sk", internal.CppPath, n)
			bytes := testutil.ReadSnapshot(t, filename)
			assert.Equal(t, bytes, sl8)
		}
	}
}

func setCompactFlag(flags byte) byte {
	return flags | compactFlagMask
}

func TestDeserializeCopiesRegisters(t *testing.T) {
	for _, tgt := range []TgtHllType{TgtHllTypeHll4, TgtHllTypeHll6, TgtHllTypeHll8} {
		sk, err := NewHllSketch(defaultLgK, tgt)
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			assert.NoError(t, sk.UpdateUInt64(uint64(i)))
		}
		b, err := sk.ToUpdatableSlice()
		assert.NoError(t, err)
		decoded, err := NewHllSketchFromSlice(b, true)
		assert.NoError(t, err)
		want, err := decoded.ToCompactSlice()
		assert.NoError(t, err)
		for i := hllByteArrStart; i < len(b); i++ {
			b[i] = 0
		}
		got, err := decoded.ToCompactSlice()
		assert.NoError(t, err)
		assert.Equal(t, want, got, "type %d", tgt)
	}
}

func serializedSketch(tb testing.TB, lgK int, tgt TgtHllType, n int, compact bool) []byte {
	tb.Helper()
	sk, err := NewHllSketch(lgK, tgt)
	assert.NoError(tb, err)
	for i := 0; i < n; i++ {
		assert.NoError(tb, sk.UpdateUInt64(uint64(i)))
	}
	var b []byte
	if compact {
		b, err = sk.ToCompactSlice()
	} else {
		b, err = sk.ToUpdatableSlice()
	}
	assert.NoError(tb, err)
	return b
}

func TestDeserializeRejectsInconsistentState(t *testing.T) {
	t.Run("list has empty or duplicate coupon", func(t *testing.T) {
		b := serializedSketch(t, 12, TgtHllTypeHll8, 5, true)
		assert.Equal(t, curModeList, extractCurMode(b))
		empty := append([]byte(nil), b...)
		copy(empty[listIntArrStart+4:listIntArrStart+8], []byte{0, 0, 0, 0})
		_, err := NewHllSketchFromSlice(empty, true)
		assert.Error(t, err, "empty")
		dup := append([]byte(nil), b...)
		copy(dup[listIntArrStart+4:listIntArrStart+8], dup[listIntArrStart:listIntArrStart+4])
		_, err = NewHllSketchFromSlice(dup, true)
		assert.Error(t, err, "duplicate")
	})

	t.Run("set count does not match table", func(t *testing.T) {
		b := serializedSketch(t, 12, TgtHllTypeHll8, 20, false)
		assert.Equal(t, curModeSet, extractCurMode(b))
		insertHashSetCount(b, 0)
		_, err := NewHllSketchFromSlice(b, true)
		assert.Error(t, err)
	})

	t.Run("set table has duplicate coupons", func(t *testing.T) {
		b := serializedSketch(t, 12, TgtHllTypeHll8, 20, false)
		var filled []int
		for off := hashSetIntArrStart; off < len(b); off += 4 {
			if b[off] != 0 || b[off+1] != 0 || b[off+2] != 0 || b[off+3] != 0 {
				filled = append(filled, off)
			}
		}
		assert.GreaterOrEqual(t, len(filled), 2)
		copy(b[filled[1]:filled[1]+4], b[filled[0]:filled[0]+4])
		_, err := NewHllSketchFromSlice(b, true)
		assert.Error(t, err)
	})

	t.Run("hll4 aux token without aux entry", func(t *testing.T) {
		b := serializedSketch(t, 12, TgtHllTypeHll4, 1000, true)
		assert.Equal(t, curModeHll, extractCurMode(b))
		assert.Zero(t, extractAuxCount(b))
		b[hllByteArrStart] = 0xff
		_, err := NewHllSketchFromSlice(b, true)
		assert.Error(t, err)
	})

	t.Run("hll8 register out of range", func(t *testing.T) {
		b := serializedSketch(t, 12, TgtHllTypeHll8, 1000, true)
		assert.Equal(t, curModeHll, extractCurMode(b))
		b[hllByteArrStart] = 0x40
		_, err := NewHllSketchFromSlice(b, true)
		assert.Error(t, err)
	})
}

func TestRejectInvalidTgtHllType(t *testing.T) {
	for _, tgt := range []TgtHllType{-1, 3} {
		_, err := NewHllSketch(12, tgt)
		assert.Error(t, err, "type %d", tgt)
	}

	list, err := newCouponList(12, TgtHllType(3), curModeList)
	assert.NoError(t, err)
	_, err = promoteListToHll(&list)
	assert.Error(t, err)

	set, err := newCouponHashSet(12, TgtHllType(3))
	assert.NoError(t, err)
	_, err = promoteSetToHll(&set)
	assert.Error(t, err)
}

func TestNewUnionFromSliceRejectsShortInput(t *testing.T) {
	b := serializedSketch(t, 12, TgtHllTypeHll8, 5, true)
	for l := 0; l < 8; l++ {
		assert.NotPanics(t, func() {
			_, err := NewUnionFromSlice(b[:l])
			assert.Error(t, err)
		}, "len %d", l)
	}
}

func FuzzNewHllSketchFromSlice(f *testing.F) {
	for _, lgK := range []int{8, 12} {
		for _, tgt := range []TgtHllType{TgtHllTypeHll4, TgtHllTypeHll6, TgtHllTypeHll8} {
			for _, n := range []int{5, 20, 1000} { // LIST, SET, HLL
				f.Add(serializedSketch(f, lgK, tgt, n, true))
				f.Add(serializedSketch(f, lgK, tgt, n, false))
			}
		}
	}
	// Inputs found by fuzzing that used to decode and then panic on a later operation.
	// LIST: duplicate coupons and an empty slot within count.
	f.Add([]byte("B\x01\a\f00\b00000000000000000000000000000\x00\x00\x00\x00"))
	// SET: duplicate coupons in the updatable table.
	f.Add([]byte("C\x01\a\f\x05001\x14\x00\x00\x00\x00\x00\x00\x000000\x00\x00\x00\x0000000000\x00\x00\x00\x000000\x00\x00\x00\x00\x00\x00\x00\x000000\x00\x00\x00\x000000\x00\x00\x00\x0000000000\x00\x00\x00\x000000\x00\x00\x00\x000000\x00\x00\x00\x000000000000000000\x00\x00\x00\x00000000000000000000000000\x00\x00\x00\x00"))
	// HLL4: aux tokens without an aux map.
	f.Add([]byte("\n\x01\a\b0A020000000000000000000000000000\x00\x00\x00\x0000000000000000000_0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	f.Fuzz(func(t *testing.T, data []byte) {
		sk, err := NewHllSketchFromSlice(data, true)
		if err != nil {
			return
		}
		// A decoded sketch must survive every follow-up operation without panicking.
		_, _ = sk.GetEstimate()
		_, _ = sk.ToCompactSlice()
		_, _ = sk.ToUpdatableSlice()
		for _, tgt := range []TgtHllType{TgtHllTypeHll4, TgtHllTypeHll6, TgtHllTypeHll8} {
			_, _ = sk.CopyAs(tgt)
		}
		if union, err := NewUnion(sk.GetLgConfigK()); err == nil {
			_ = union.UpdateSketch(sk)
			_, _ = union.GetResult(TgtHllTypeHll8)
		}
		_, _ = NewUnionFromSlice(data)
		for i := 0; i < 100; i++ {
			_ = sk.UpdateUInt64(uint64(i))
		}
	})
}
