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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/internal"
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
	if _, err := os.Stat(path); os.IsNotExist(err) {
		assert.FailNowf(t, "file does not exist", "file %s does not exist", path)
		return
	}

	b, err := os.ReadFile(path)
	assert.NoError(t, err)
	sk, err := Decode(b)
	if err != nil {
		return
	}

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
