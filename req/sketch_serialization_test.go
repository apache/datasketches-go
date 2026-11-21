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

package req

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/internal/testutil"
)

func TestSketchSerializationDeserialization(t *testing.T) {
	k := 12
	exact := (nomCapMul * initNumberOfSections * k) - 1

	testCases := []struct {
		name  string
		hra   bool
		count int
	}{
		{"empty LRA", false, 0},
		{"empty HRA", true, 0},
		{"rawItems LRA", false, 4},
		{"rawItems HRA", true, 4},
		{"exact LRA", false, exact},
		{"exact HRA", true, exact},
		{"estimation LRA", false, 2 * exact},
		{"estimation HRA", true, 2 * exact},
	}
	for _, tc := range testCases {
		t.Run(tc.name+" MarshalBinary+Decode", func(t *testing.T) {
			sk1 := newSketchForTest(t, k, tc.hra, tc.count)

			sk1Arr, err := sk1.MarshalBinary()
			assert.NoError(t, err)

			sk2, err := Decode(sk1Arr)
			assert.NoError(t, err)

			assertSketchesEqual(t, sk1, sk2)
		})
		t.Run(tc.name+" Encoder+Decoder", func(t *testing.T) {
			sk1 := newSketchForTest(t, k, tc.hra, tc.count)

			var buf bytes.Buffer
			enc := NewEncoder(&buf)
			assert.NoError(t, enc.Encode(sk1))

			dec := NewDecoder()
			sk2, err := dec.Decode(bytes.NewReader(buf.Bytes()))
			assert.NoError(t, err)

			assertSketchesEqual(t, sk1, sk2)
		})
	}
}

func newSketchForTest(t *testing.T, k int, hra bool, count int) *Sketch {
	t.Helper()
	sk, err := NewSketch(WithK(k), WithHighRankAccuracyMode(hra))
	assert.NoError(t, err)
	for i := 1; i <= count; i++ {
		assert.NoError(t, sk.Update(float32(i)))
	}
	return sk
}

func assertSketchesEqual(t *testing.T, sk1, sk2 *Sketch) {
	t.Helper()
	assert.Equal(t, sk1.NumRetained(), sk2.NumRetained())
	assert.Equal(t, sk1.IsEmpty(), sk2.IsEmpty())
	if !sk1.IsEmpty() {
		min1, err := sk1.MinItem()
		assert.NoError(t, err)
		min2, err := sk2.MinItem()
		assert.NoError(t, err)
		assert.Equal(t, min1, min2)

		max1, err := sk1.MaxItem()
		assert.NoError(t, err)
		max2, err := sk2.MaxItem()
		assert.NoError(t, err)
		assert.Equal(t, max1, max2)
	}
	assert.Equal(t, sk1.N(), sk2.N())
	assert.Equal(t, sk1.IsHighRankAccuracyMode(), sk2.IsHighRankAccuracyMode())
	assert.Equal(t, sk1.K(), sk2.K())
	assert.Equal(t, sk1.maxNomSize, sk2.maxNomSize)
	assert.Equal(t, sk1.numLevels(), sk2.numLevels())
	assert.Equal(t, sk1.SerializedSizeBytes(), sk2.SerializedSizeBytes())
}

func TestGenerateGoSnapshots(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}
	assert.NoError(t, os.MkdirAll(internal.GoPath, os.ModePerm))

	t.Run("positive", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			sk, err := NewSketch()
			assert.NoError(t, err)
			for i := 1; i <= n; i++ {
				assert.NoError(t, sk.Update(float32(i)))
			}
			writeGoSketch(t, sk, fmt.Sprintf("req_float_n%d_go.sk", n))
		}
	})

	t.Run("negative", func(t *testing.T) {
		nArr := []int{1, 10}
		for _, n := range nArr {
			sk, err := NewSketch()
			assert.NoError(t, err)
			for i := 1; i <= n; i++ {
				assert.NoError(t, sk.Update(float32(-i)))
			}
			writeGoSketch(t, sk, fmt.Sprintf("req_float_negative_n%d_go.sk", n))
		}
	})

	t.Run("mixed", func(t *testing.T) {
		nArr := []int{1, 10}
		for _, n := range nArr {
			sk, err := NewSketch()
			assert.NoError(t, err)
			for i := -n; i <= n; i++ {
				assert.NoError(t, sk.Update(float32(i)))
			}
			writeGoSketch(t, sk, fmt.Sprintf("req_float_mixed_n%d_go.sk", n))
		}
	})
}

func writeGoSketch(t *testing.T, sk *Sketch, name string) {
	t.Helper()
	b, err := sk.MarshalBinary()
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(fmt.Sprintf("%s/%s", internal.GoPath, name), b, 0644))
}

func TestJavaCompat(t *testing.T) {
	assertReqCompat(t, internal.JavaPath, "java")
}

func TestCPPCompat(t *testing.T) {
	assertReqCompat(t, internal.CppPath, "cpp")
}

func assertReqCompat(t *testing.T, dir, lang string) {
	t.Helper()

	t.Run("positive", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			path := fmt.Sprintf("%s/req_float_n%d_%s.sk", dir, n, lang)
			assertReqSketchFromFile(t, path, int64(n), n == 0, n > 10, 1, float32(n))
		}
	})

	t.Run("negative", func(t *testing.T) {
		nArr := []int{1, 10}
		for _, n := range nArr {
			path := fmt.Sprintf("%s/req_float_negative_n%d_%s.sk", dir, n, lang)
			assertReqSketchFromFile(t, path, int64(n), false, false, float32(-n), -1)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		nArr := []int{1, 10}
		for _, n := range nArr {
			path := fmt.Sprintf("%s/req_float_mixed_n%d_%s.sk", dir, n, lang)
			assertReqSketchFromFile(t, path, int64(2*n+1), false, false, float32(-n), float32(n))
		}
	})
}

func assertReqSketchFromFile(t *testing.T, path string, expectN int64, expectEmpty, expectEstimation bool, expectMin, expectMax float32) {
	t.Helper()
	b := testutil.ReadSnapshot(t, path)
	sk, err := Decode(b)
	require.NoError(t, err, path)

	decoder := NewDecoder()
	streamSk, err := decoder.Decode(bytes.NewReader(b))
	require.NoError(t, err, path)
	assert.Equal(t, sk.N(), streamSk.N(), path)

	assert.True(t, sk.IsHighRankAccuracyMode())
	assert.Equal(t, expectEmpty, sk.IsEmpty())
	assert.Equal(t, expectEstimation, sk.IsEstimationMode())
	assert.Equal(t, expectN, sk.N())
	if expectEmpty {
		return
	}

	minItem, err := sk.MinItem()
	assert.NoError(t, err)
	assert.Equal(t, expectMin, minItem)

	maxItem, err := sk.MaxItem()
	assert.NoError(t, err)
	assert.Equal(t, expectMax, maxItem)

	var totalWeight int64
	for item := range sk.All() {
		assert.GreaterOrEqual(t, item.Quantile, minItem)
		assert.LessOrEqual(t, item.Quantile, maxItem)
		totalWeight += item.Weight
	}
	assert.Equal(t, sk.N(), totalWeight)
}

func TestDecodeRejectsInconsistentCompactors(t *testing.T) {
	const (
		flagsOffset         = 3
		rawItemsFlag        = 16
		kOffset             = 4
		numLevelsOffset     = 6
		numRawItemsOffset   = 7
		nOffset             = 8
		exactCompactor      = 8
		estimationCompactor = 24
		sectionSizeOffset   = 8
		lgWeightOffset      = 12
		numSectionsOffset   = 13
		numItemsOffset      = 16
		itemsOffset         = 20
	)
	serialize := func(t *testing.T, k, n int, hra bool) []byte {
		t.Helper()
		b, err := newSketchForTest(t, k, hra, n).MarshalBinary()
		require.NoError(t, err)
		return b
	}
	raw := func(t *testing.T) []byte { return serialize(t, 12, 3, true) }
	exact := func(t *testing.T) []byte { return serialize(t, 12, 50, true) }
	estimation := func(t *testing.T) []byte {
		b := serialize(t, 12, 1000, true)
		require.Greater(t, b[numLevelsOffset], byte(2))
		return b
	}
	putFloat := func(b []byte, offset int, v float32) {
		binary.LittleEndian.PutUint32(b[offset:], math.Float32bits(v))
	}
	checkAccepted := func(t *testing.T, b []byte, n int64) {
		t.Helper()
		sk, err := Decode(b)
		require.NoError(t, err)
		assert.Equal(t, n, sk.N())
		decoder := NewDecoder()
		sk, err = decoder.Decode(bytes.NewReader(b))
		require.NoError(t, err)
		assert.Equal(t, n, sk.N())
	}
	checkRejected := func(t *testing.T, b []byte, msgAndArgs ...any) {
		t.Helper()
		_, err := Decode(b)
		assert.Error(t, err, msgAndArgs...)
		decoder := NewDecoder()
		_, err = decoder.Decode(bytes.NewReader(b))
		assert.Error(t, err, msgAndArgs...)
	}

	t.Run("valid images", func(t *testing.T) {
		for _, hra := range []bool{true, false} {
			sk := newSketchForTest(t, 12, hra, 1_000_000)
			b, err := sk.MarshalBinary()
			require.NoError(t, err)
			require.Greater(t, b[estimationCompactor+numSectionsOffset], byte(initNumberOfSections))
			checkAccepted(t, b, sk.N())
		}
	})

	t.Run("valid images with the smallest section size", func(t *testing.T) {
		b := serialize(t, 6, 100_000, true)
		putFloat(b, estimationCompactor+sectionSizeOffset, 3)
		b[estimationCompactor+numSectionsOffset] = 12
		checkAccepted(t, b, 100_000)

		b = estimation(t)
		putFloat(b, estimationCompactor+sectionSizeOffset, 3)
		b[estimationCompactor+numSectionsOffset] = 48
		checkAccepted(t, b, 1000)
	})

	t.Run("invalid k", func(t *testing.T) {
		for _, k := range []uint16{2, 13, 2000} {
			b := estimation(t)
			binary.LittleEndian.PutUint16(b[kOffset:], k)
			checkRejected(t, b, "k %d", k)
		}
	})

	t.Run("section size out of range", func(t *testing.T) {
		for _, size := range []float32{0, 2, 14, 1e12, float32(math.NaN())} {
			b := estimation(t)
			putFloat(b, estimationCompactor+sectionSizeOffset, size)
			checkRejected(t, b, "section size %v", size)
		}
	})

	t.Run("number of sections out of range", func(t *testing.T) {
		for _, sections := range []byte{0, 65} {
			b := estimation(t)
			b[estimationCompactor+numSectionsOffset] = sections
			checkRejected(t, b, "sections %d", sections)
		}
		b := estimation(t)
		putFloat(b, estimationCompactor+sectionSizeOffset, 3)
		b[estimationCompactor+numSectionsOffset] = 64
		checkRejected(t, b, "too many sections for the section size and k")
	})

	t.Run("wrong lg weight", func(t *testing.T) {
		for _, weight := range []byte{1, 70} {
			b := estimation(t)
			b[estimationCompactor+lgWeightOffset] = weight
			checkRejected(t, b, "lg weight %d", weight)
		}
		b := exact(t)
		b[exactCompactor+lgWeightOffset] = 1
		checkRejected(t, b, "exact lg weight")

		b = estimation(t)
		count := binary.LittleEndian.Uint32(b[estimationCompactor+numItemsOffset:])
		secondCompactor := estimationCompactor + itemsOffset + 4*int(count)
		require.Equal(t, byte(1), b[secondCompactor+lgWeightOffset])
		b[secondCompactor+lgWeightOffset] = 0
		checkRejected(t, b, "second compactor repeats the first lg weight")
	})

	t.Run("no levels", func(t *testing.T) {
		b := exact(t)
		b[numLevelsOffset] = 0
		checkRejected(t, b, "exact")
		b = raw(t)
		b[numLevelsOffset] = 0
		checkRejected(t, b, "raw")
	})

	t.Run("no items", func(t *testing.T) {
		b := exact(t)
		binary.LittleEndian.PutUint32(b[exactCompactor+numItemsOffset:], 0)
		checkRejected(t, b, "exact")
		b = raw(t)
		b[numRawItemsOffset] = 0
		checkRejected(t, b, "raw")
	})

	t.Run("compactor header beyond the input", func(t *testing.T) {
		checkRejected(t, estimation(t)[:estimationCompactor+10])
	})

	t.Run("raw items with more than one compactor", func(t *testing.T) {
		b := estimation(t)
		b[flagsOffset] |= rawItemsFlag
		checkRejected(t, b)
	})

	t.Run("item count beyond the input", func(t *testing.T) {
		b := exact(t)
		binary.LittleEndian.PutUint32(b[exactCompactor+numItemsOffset:], math.MaxUint32)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		checkRejected(t, b)
		runtime.ReadMemStats(&after)
		assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
	})

	t.Run("estimation with the exact preamble size", func(t *testing.T) {
		b := estimation(t)
		b[0] = 2
		checkRejected(t, b)
	})

	withCompactors := func(t *testing.T, n uint64, counts []uint32) []byte {
		t.Helper()
		b := append([]byte(nil), estimation(t)[:estimationCompactor]...)
		b[numLevelsOffset] = byte(len(counts))
		binary.LittleEndian.PutUint64(b[nOffset:], n)
		for level, count := range counts {
			c := make([]byte, itemsOffset+4*int(count))
			putFloat(c, sectionSizeOffset, 12)
			c[lgWeightOffset] = byte(level)
			c[numSectionsOffset] = 3
			binary.LittleEndian.PutUint32(c[numItemsOffset:], count)
			b = append(b, c...)
		}
		return b
	}

	t.Run("estimation with no retained items", func(t *testing.T) {
		checkAccepted(t, withCompactors(t, 5, []uint32{1, 2}), 5)
		checkRejected(t, withCompactors(t, 1, []uint32{0, 0}))
	})

	t.Run("retained weight does not match n", func(t *testing.T) {
		b := estimation(t)
		n := binary.LittleEndian.Uint64(b[nOffset:])
		binary.LittleEndian.PutUint64(b[nOffset:], n+1)
		checkRejected(t, b)
	})

	t.Run("retained weight beyond the maximum n", func(t *testing.T) {
		counts := make([]uint32, 64)
		counts[63] = 1
		checkRejected(t, withCompactors(t, 1<<63, counts))
	})
}
