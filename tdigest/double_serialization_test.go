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

package tdigest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/internal/testutil"
)

func TestGenerateGoSnapshots(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}
	assert.NoError(t, os.MkdirAll(internal.GoPath, os.ModePerm))

	t.Run("Without Buffer", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewDouble(100)
			assert.NoError(t, err)

			for i := 1; i <= n; i++ {
				assert.NoError(t, sketch.Update(float64(i)))
			}

			b, err := EncodeDouble(sketch, false)
			assert.NoError(t, err)

			filename := fmt.Sprintf("%s/tdigest_double_n%d_go.sk", internal.GoPath, n)
			err = os.WriteFile(filename, b, 0644)
			assert.NoError(t, err)
			t.Logf("Generated: %s", filename)
		}
	})

	t.Run("With Buffer", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewDouble(100)
			assert.NoError(t, err)

			for i := 1; i <= n; i++ {
				err := sketch.Update(float64(i))
				assert.NoError(t, err)
			}

			b, err := EncodeDouble(sketch, true)
			assert.NoError(t, err)

			filename := fmt.Sprintf("%s/tdigest_double_buf_n%d_go.sk", internal.GoPath, n)
			err = os.WriteFile(filename, b, 0644)
			assert.NoError(t, err)
			t.Logf("Generated: %s", filename)
		}
	})
}

func TestDoubleJavaCompat(t *testing.T) {
	ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range ns {
		filename := fmt.Sprintf("%s/tdigest_double_n%d_java.sk", internal.JavaPath, n)
		b := testutil.ReadSnapshot(t, filename)

		sketch, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, n == 0, sketch.IsEmpty())
		assert.Equal(t, uint64(n), sketch.TotalWeight())
		if n > 0 {
			minVal, err := sketch.MinValue()
			assert.NoError(t, err)
			assert.Equal(t, 1.0, minVal)

			maxVal, err := sketch.MaxValue()
			assert.NoError(t, err)
			assert.Equal(t, float64(n), maxVal)

			rank, err := sketch.Rank(0)
			assert.NoError(t, err)
			assert.Equal(t, float64(0), rank)

			rank, err = sketch.Rank(float64(n + 1))
			assert.NoError(t, err)
			assert.Equal(t, float64(1), rank)

			if n == 1 {
				rank, err = sketch.Rank(float64(n))
				assert.NoError(t, err)
				assert.Equal(t, 0.5, rank)
			} else {
				rank, err = sketch.Rank(float64(n) / 2.0)
				assert.NoError(t, err)
				assert.InDelta(t, 0.5, rank, 0.05)
			}
		}
	}
}

func TestDoubleCPPCompat(t *testing.T) {
	t.Run("Without Buffer", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/tdigest_double_n%d_cpp.sk", internal.CppPath, n)
			b := testutil.ReadSnapshot(t, filename)

			sketch, err := DecodeDouble(b)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, uint64(n), sketch.TotalWeight())
			if n > 0 {
				minVal, err := sketch.MinValue()
				assert.NoError(t, err)
				assert.Equal(t, 1.0, minVal)

				maxVal, err := sketch.MaxValue()
				assert.NoError(t, err)
				assert.Equal(t, float64(n), maxVal)

				rank, err := sketch.Rank(0)
				assert.NoError(t, err)
				assert.Equal(t, float64(0), rank)

				rank, err = sketch.Rank(float64(n + 1))
				assert.NoError(t, err)
				assert.Equal(t, float64(1), rank)

				if n == 1 {
					rank, err = sketch.Rank(float64(n))
					assert.NoError(t, err)
					assert.Equal(t, 0.5, rank)
				} else {
					rank, err = sketch.Rank(float64(n) / 2.0)
					assert.NoError(t, err)
					assert.InDelta(t, 0.5, rank, 0.05)
				}
			}
		}
	})

	t.Run("With Buffer", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/tdigest_double_buf_n%d_cpp.sk", internal.CppPath, n)
			b := testutil.ReadSnapshot(t, filename)

			sketch, err := DecodeDouble(b)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, uint64(n), sketch.TotalWeight())
			if n > 0 {
				minVal, err := sketch.MinValue()
				assert.NoError(t, err)
				assert.Equal(t, 1.0, minVal)

				maxVal, err := sketch.MaxValue()
				assert.NoError(t, err)
				assert.Equal(t, float64(n), maxVal)

				rank, err := sketch.Rank(0)
				assert.NoError(t, err)
				assert.Equal(t, float64(0), rank)

				rank, err = sketch.Rank(float64(n + 1))
				assert.NoError(t, err)
				assert.Equal(t, float64(1), rank)

				if n == 1 {
					rank, err = sketch.Rank(float64(n))
					assert.NoError(t, err)
					assert.Equal(t, 0.5, rank)
				} else {
					rank, err = sketch.Rank(float64(n) / 2.0)
					assert.NoError(t, err)
					assert.InDelta(t, 0.5, rank, 0.05)
				}
			}
		}
	})
}

func TestDoubleEncoderAndDoubleDecoder(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, false)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded, err := dec.Decode(&buf)
		assert.NoError(t, err)

		assert.Equal(t, sk.K(), decoded.K())
		assert.Equal(t, sk.TotalWeight(), decoded.TotalWeight())
		assert.Equal(t, sk.IsEmpty(), decoded.IsEmpty())
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		err = sk.Update(123)
		assert.NoError(t, err)

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, false)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded, err := dec.Decode(&buf)
		assert.NoError(t, err)

		assert.Equal(t, uint16(200), decoded.K())
		assert.Equal(t, uint64(1), decoded.TotalWeight())
		assert.False(t, decoded.IsEmpty())

		minVal, err := decoded.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(123), minVal)

		maxVal, err := decoded.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(123), maxVal)
	})

	t.Run("Single Value With Buffer", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		err = sk.Update(123)
		assert.NoError(t, err)

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, true)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded, err := dec.Decode(&buf)
		assert.NoError(t, err)

		assert.Equal(t, uint16(200), decoded.K())
		assert.Equal(t, uint64(1), decoded.TotalWeight())
		assert.False(t, decoded.IsEmpty())

		minVal, err := decoded.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(123), minVal)

		maxVal, err := decoded.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(123), maxVal)
	})

	t.Run("Multiple Values", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			err := sk.Update(float64(i))
			assert.NoError(t, err)
		}

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, false)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded, err := dec.Decode(&buf)
		assert.NoError(t, err)

		assert.Equal(t, sk.K(), decoded.K())
		assert.Equal(t, sk.TotalWeight(), decoded.TotalWeight())
		assert.Equal(t, sk.IsEmpty(), decoded.IsEmpty())

		expectedMinVal, err := sk.MinValue()
		assert.NoError(t, err)
		resultMinVal, err := decoded.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, expectedMinVal, resultMinVal)

		expectedMaxVal, err := sk.MaxValue()
		assert.NoError(t, err)
		resultMaxVal, err := decoded.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, expectedMaxVal, resultMaxVal)

		expectedRank, err := sk.Rank(500)
		assert.NoError(t, err)
		resultRank, err := decoded.Rank(500)
		assert.NoError(t, err)
		assert.Equal(t, expectedRank, resultRank)

		expectedQuantile, err := sk.Quantile(0.5)
		assert.NoError(t, err)
		resultQuantile, err := decoded.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, expectedQuantile, resultQuantile)
	})

	t.Run("Multiple Values With Buffer", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			assert.NoError(t, sk.Update(float64(i)))
		}

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, true)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded, err := dec.Decode(&buf)
		assert.NoError(t, err)

		assert.Equal(t, sk.K(), decoded.K())
		assert.Equal(t, sk.TotalWeight(), decoded.TotalWeight())
		assert.Equal(t, sk.IsEmpty(), decoded.IsEmpty())

		expectedMinVal, err := sk.MinValue()
		assert.NoError(t, err)
		resultMinVal, err := decoded.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, expectedMinVal, resultMinVal)

		expectedMaxVal, err := sk.MaxValue()
		assert.NoError(t, err)
		resultMaxVal, err := decoded.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, expectedMaxVal, resultMaxVal)

		expectedRank, err := sk.Rank(500)
		assert.NoError(t, err)
		resultRank, err := decoded.Rank(500)
		assert.NoError(t, err)
		assert.Equal(t, expectedRank, resultRank)

		expectedQuantile, err := sk.Quantile(0.5)
		assert.NoError(t, err)
		resultQuantile, err := decoded.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, expectedQuantile, resultQuantile)
	})

	t.Run("Compat Double", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "tdigest_ref_k100_n10000_double.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)
		buf := bytes.NewBuffer(b)

		dec := NewDoubleDecoder()
		sketch, err := dec.Decode(buf)
		assert.NoError(t, err)

		n := 10000
		assert.Equal(t, uint64(n), sketch.TotalWeight())

		minVal, err := sketch.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(0), minVal)

		maxVal, err := sketch.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(n-1), maxVal)

		rank, err := sketch.Rank(0)
		assert.NoError(t, err)
		assert.InDelta(t, 0, rank, 0.0001)

		rank, err = sketch.Rank(float64(n) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.25, rank, 0.0001)

		rank, err = sketch.Rank(float64(n) / 2.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.5, rank, 0.0001)

		rank, err = sketch.Rank(float64(n*3) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.75, rank, 0.0001)

		rank, err = sketch.Rank(float64(n))
		assert.NoError(t, err)
		assert.Equal(t, float64(1), rank)
	})

	t.Run("Compat Float", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "tdigest_ref_k100_n10000_float.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)
		buf := bytes.NewBuffer(b)

		dec := NewDoubleDecoder()
		sketch, err := dec.Decode(buf)
		assert.NoError(t, err)

		n := 10000
		assert.Equal(t, uint64(n), sketch.TotalWeight())

		minVal, err := sketch.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(0), minVal)

		maxVal, err := sketch.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(n-1), maxVal)

		rank, err := sketch.Rank(0)
		assert.NoError(t, err)
		assert.InDelta(t, 0, rank, 0.0001)

		rank, err = sketch.Rank(float64(n) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.25, rank, 0.0001)

		rank, err = sketch.Rank(float64(n) / 2.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.5, rank, 0.0001)

		rank, err = sketch.Rank(float64(n*3) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.75, rank, 0.0001)

		rank, err = sketch.Rank(float64(n))
		assert.NoError(t, err)
		assert.Equal(t, float64(1), rank)
	})
}

func TestEncodeDoubleAndDecodeDouble(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		decoded, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, sk.K(), decoded.K())
		assert.Equal(t, sk.TotalWeight(), decoded.TotalWeight())
		assert.Equal(t, sk.IsEmpty(), decoded.IsEmpty())
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(123))

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		decoded, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, uint16(200), decoded.K())
		assert.Equal(t, uint64(1), decoded.TotalWeight())
		assert.False(t, decoded.IsEmpty())

		minVal, err := decoded.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(123), minVal)

		maxVal, err := decoded.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(123), maxVal)
	})

	t.Run("Single Value With Buffer", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(123))

		b, err := EncodeDouble(sk, true)
		assert.NoError(t, err)

		decoded, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, uint16(200), decoded.K())
		assert.Equal(t, uint64(1), decoded.TotalWeight())
		assert.False(t, decoded.IsEmpty())

		minVal, err := decoded.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(123), minVal)

		maxVal, err := decoded.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(123), maxVal)
	})

	t.Run("Multiple Values", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			assert.NoError(t, sk.Update(float64(i)))
		}

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		decoded, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, sk.K(), decoded.K())
		assert.Equal(t, sk.TotalWeight(), decoded.TotalWeight())
		assert.Equal(t, sk.IsEmpty(), decoded.IsEmpty())

		expectedMinVal, err := sk.MinValue()
		assert.NoError(t, err)
		resultMinVal, err := decoded.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, expectedMinVal, resultMinVal)

		expectedMaxVal, err := sk.MaxValue()
		assert.NoError(t, err)
		resultMaxVal, err := decoded.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, expectedMaxVal, resultMaxVal)

		expectedRank, err := sk.Rank(500)
		assert.NoError(t, err)
		resultRank, err := decoded.Rank(500)
		assert.NoError(t, err)
		assert.Equal(t, expectedRank, resultRank)

		expectedQuantile, err := sk.Quantile(0.5)
		assert.NoError(t, err)
		resultQuantile, err := decoded.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, expectedQuantile, resultQuantile)
	})

	t.Run("Multiple Values With Buffer", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			assert.NoError(t, sk.Update(float64(i)))
		}

		b, err := EncodeDouble(sk, true)
		assert.NoError(t, err)

		decoded, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, sk.K(), decoded.K())
		assert.Equal(t, sk.TotalWeight(), decoded.TotalWeight())
		assert.Equal(t, sk.IsEmpty(), decoded.IsEmpty())

		expectedMinVal, err := sk.MinValue()
		assert.NoError(t, err)
		resultMinVal, err := decoded.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, expectedMinVal, resultMinVal)

		expectedMaxVal, err := sk.MaxValue()
		assert.NoError(t, err)
		resultMaxVal, err := decoded.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, expectedMaxVal, resultMaxVal)

		expectedRank, err := sk.Rank(500)
		assert.NoError(t, err)
		resultRank, err := decoded.Rank(500)
		assert.NoError(t, err)
		assert.Equal(t, expectedRank, resultRank)

		expectedQuantile, err := sk.Quantile(0.5)
		assert.NoError(t, err)
		resultQuantile, err := decoded.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, expectedQuantile, resultQuantile)
	})

	t.Run("Compat Double", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "tdigest_ref_k100_n10000_double.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)

		sketch, err := DecodeDouble(b)
		assert.NoError(t, err)

		n := 10000
		assert.Equal(t, uint64(n), sketch.TotalWeight())

		minVal, err := sketch.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(0), minVal)

		maxVal, err := sketch.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(n-1), maxVal)

		rank, err := sketch.Rank(0)
		assert.NoError(t, err)
		assert.InDelta(t, 0, rank, 0.0001)

		rank, err = sketch.Rank(float64(n) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.25, rank, 0.0001)

		rank, err = sketch.Rank(float64(n) / 2.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.5, rank, 0.0001)

		rank, err = sketch.Rank(float64(n*3) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.75, rank, 0.0001)

		rank, err = sketch.Rank(float64(n))
		assert.NoError(t, err)
		assert.Equal(t, float64(1), rank)
	})

	t.Run("Compat Float", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "tdigest_ref_k100_n10000_float.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)

		sketch, err := DecodeDouble(b)
		assert.NoError(t, err)

		n := 10000
		assert.Equal(t, uint64(n), sketch.TotalWeight())

		minVal, err := sketch.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(0), minVal)

		maxVal, err := sketch.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(n-1), maxVal)

		rank, err := sketch.Rank(0)
		assert.NoError(t, err)
		assert.InDelta(t, 0, rank, 0.0001)

		rank, err = sketch.Rank(float64(n) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.25, rank, 0.0001)

		rank, err = sketch.Rank(float64(n) / 2.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.5, rank, 0.0001)

		rank, err = sketch.Rank(float64(n*3) / 4.0)
		assert.NoError(t, err)
		assert.InDelta(t, 0.75, rank, 0.0001)

		rank, err = sketch.Rank(float64(n))
		assert.NoError(t, err)
		assert.Equal(t, float64(1), rank)
	})
}

func TestEncodeDoubleEquivalence(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, false)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		assert.Equal(t, buf.Bytes(), b)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(buf.Bytes()))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(123))

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, false)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		assert.Equal(t, buf.Bytes(), b)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(buf.Bytes()))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())

		minVal1, err := decoded1.MinValue()
		assert.NoError(t, err)
		minVal2, err := decoded2.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, minVal1, minVal2)

		maxVal1, err := decoded1.MaxValue()
		assert.NoError(t, err)
		maxVal2, err := decoded2.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, maxVal1, maxVal2)
	})

	t.Run("Single Value With Buffer", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(123))

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, true)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		b, err := EncodeDouble(sk, true)
		assert.NoError(t, err)

		assert.Equal(t, buf.Bytes(), b)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(buf.Bytes()))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())

		minVal1, err := decoded1.MinValue()
		assert.NoError(t, err)
		minVal2, err := decoded2.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, minVal1, minVal2)

		maxVal1, err := decoded1.MaxValue()
		assert.NoError(t, err)
		maxVal2, err := decoded2.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, maxVal1, maxVal2)
	})

	t.Run("Multiple Values", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			assert.NoError(t, sk.Update(float64(i)))
		}

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, false)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		assert.Equal(t, buf.Bytes(), b)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(buf.Bytes()))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())

		minVal1, err := decoded1.MinValue()
		assert.NoError(t, err)
		minVal2, err := decoded2.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, minVal1, minVal2)

		maxVal1, err := decoded1.MaxValue()
		assert.NoError(t, err)
		maxVal2, err := decoded2.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, maxVal1, maxVal2)

		rank1, err := decoded1.Rank(500)
		assert.NoError(t, err)
		rank2, err := decoded2.Rank(500)
		assert.NoError(t, err)
		assert.Equal(t, rank1, rank2)

		quantile1, err := decoded1.Quantile(0.5)
		assert.NoError(t, err)
		quantile2, err := decoded2.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, quantile1, quantile2)
	})

	t.Run("Multiple Values With Buffer", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			assert.NoError(t, sk.Update(float64(i)))
		}

		var buf bytes.Buffer
		enc := NewDoubleEncoder(&buf, true)
		err = enc.Encode(sk)
		assert.NoError(t, err)

		b, err := EncodeDouble(sk, true)
		assert.NoError(t, err)

		assert.Equal(t, buf.Bytes(), b)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(buf.Bytes()))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())

		minVal1, err := decoded1.MinValue()
		assert.NoError(t, err)
		minVal2, err := decoded2.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, minVal1, minVal2)

		maxVal1, err := decoded1.MaxValue()
		assert.NoError(t, err)
		maxVal2, err := decoded2.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, maxVal1, maxVal2)

		rank1, err := decoded1.Rank(500)
		assert.NoError(t, err)
		rank2, err := decoded2.Rank(500)
		assert.NoError(t, err)
		assert.Equal(t, rank1, rank2)

		quantile1, err := decoded1.Quantile(0.5)
		assert.NoError(t, err)
		quantile2, err := decoded2.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, quantile1, quantile2)
	})
}

func TestDecodeDoubleEquivalence(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(b))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())
	})

	t.Run("Single Value", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(123))

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(b))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())

		minVal1, err := decoded1.MinValue()
		assert.NoError(t, err)
		minVal2, err := decoded2.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, minVal1, minVal2)

		maxVal1, err := decoded1.MaxValue()
		assert.NoError(t, err)
		maxVal2, err := decoded2.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, maxVal1, maxVal2)
	})

	t.Run("Single Value With Buffer", func(t *testing.T) {
		sk, err := NewDouble(DefaultK)
		assert.NoError(t, err)
		assert.NoError(t, sk.Update(123))

		b, err := EncodeDouble(sk, true)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(b))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())

		minVal1, err := decoded1.MinValue()
		assert.NoError(t, err)
		minVal2, err := decoded2.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, minVal1, minVal2)

		maxVal1, err := decoded1.MaxValue()
		assert.NoError(t, err)
		maxVal2, err := decoded2.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, maxVal1, maxVal2)
	})

	t.Run("Multiple Values", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			assert.NoError(t, sk.Update(float64(i)))
		}

		b, err := EncodeDouble(sk, false)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(b))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())

		minVal1, err := decoded1.MinValue()
		assert.NoError(t, err)
		minVal2, err := decoded2.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, minVal1, minVal2)

		maxVal1, err := decoded1.MaxValue()
		assert.NoError(t, err)
		maxVal2, err := decoded2.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, maxVal1, maxVal2)

		rank1, err := decoded1.Rank(500)
		assert.NoError(t, err)
		rank2, err := decoded2.Rank(500)
		assert.NoError(t, err)
		assert.Equal(t, rank1, rank2)

		quantile1, err := decoded1.Quantile(0.5)
		assert.NoError(t, err)
		quantile2, err := decoded2.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, quantile1, quantile2)
	})

	t.Run("Multiple Values With Buffer", func(t *testing.T) {
		sk, err := NewDouble(100)
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			assert.NoError(t, sk.Update(float64(i)))
		}

		b, err := EncodeDouble(sk, true)
		assert.NoError(t, err)

		dec := NewDoubleDecoder()
		decoded1, err := dec.Decode(bytes.NewReader(b))
		assert.NoError(t, err)

		decoded2, err := DecodeDouble(b)
		assert.NoError(t, err)

		assert.Equal(t, decoded1.K(), decoded2.K())
		assert.Equal(t, decoded1.TotalWeight(), decoded2.TotalWeight())
		assert.Equal(t, decoded1.IsEmpty(), decoded2.IsEmpty())

		minVal1, err := decoded1.MinValue()
		assert.NoError(t, err)
		minVal2, err := decoded2.MinValue()
		assert.NoError(t, err)
		assert.Equal(t, minVal1, minVal2)

		maxVal1, err := decoded1.MaxValue()
		assert.NoError(t, err)
		maxVal2, err := decoded2.MaxValue()
		assert.NoError(t, err)
		assert.Equal(t, maxVal1, maxVal2)

		rank1, err := decoded1.Rank(500)
		assert.NoError(t, err)
		rank2, err := decoded2.Rank(500)
		assert.NoError(t, err)
		assert.Equal(t, rank1, rank2)

		quantile1, err := decoded1.Quantile(0.5)
		assert.NoError(t, err)
		quantile2, err := decoded2.Quantile(0.5)
		assert.NoError(t, err)
		assert.Equal(t, quantile1, quantile2)
	})
}

func TestDecodeDouble_InvalidData(t *testing.T) {
	t.Run("Min is NaN", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 0) // 1 centroid, 0 buffered
		// Set min to NaN (offset 16-23)
		binary.LittleEndian.PutUint64(data[16:], math.Float64bits(math.NaN()))

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "min")
		assert.Contains(t, err.Error(), "NaN")
	})

	t.Run("Max is NaN", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 0) // 1 centroid, 0 buffered
		// Set max to NaN (offset 24-31)
		binary.LittleEndian.PutUint64(data[24:], math.Float64bits(math.NaN()))

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max")
		assert.Contains(t, err.Error(), "NaN")
	})

	t.Run("Centroid Mean is NaN", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 0) // 1 centroid, 0 buffered
		// Set centroid mean to NaN (offset 32-39)
		binary.LittleEndian.PutUint64(data[32:], math.Float64bits(math.NaN()))

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "centroid mean")
		assert.Contains(t, err.Error(), "NaN")
	})

	t.Run("Centroid Mean is Positive Infinity", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 0) // 1 centroid, 0 buffered
		// Set centroid mean to +Inf (offset 32-39)
		binary.LittleEndian.PutUint64(data[32:], math.Float64bits(math.Inf(1)))

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "centroid mean")
		assert.Contains(t, err.Error(), "Inf")
	})

	t.Run("Centroid Mean is Negative Infinity", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 0) // 1 centroid, 0 buffered
		// Set centroid mean to -Inf (offset 32-39)
		binary.LittleEndian.PutUint64(data[32:], math.Float64bits(math.Inf(-1)))

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "centroid mean")
		assert.Contains(t, err.Error(), "Inf")
	})

	t.Run("Centroid Weight is Zero", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 0) // 1 centroid, 0 buffered
		// Set centroid weight to zero (offset 40-47)
		binary.LittleEndian.PutUint64(data[40:], 0)

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "centroid weight")
		assert.Contains(t, err.Error(), "Zero")
	})

	t.Run("Buffered Value is NaN", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 1) // 1 centroid, 1 buffered
		// Set buffered value to NaN (offset 48-55, after centroid)
		binary.LittleEndian.PutUint64(data[48:], math.Float64bits(math.NaN()))

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "buffered value")
		assert.Contains(t, err.Error(), "NaN")
	})

	t.Run("Buffered Value is Positive Infinity", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 1) // 1 centroid, 1 buffered
		// Set buffered value to +Inf (offset 48-55, after centroid)
		binary.LittleEndian.PutUint64(data[48:], math.Float64bits(math.Inf(1)))

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "buffered value")
		assert.Contains(t, err.Error(), "Inf")
	})

	t.Run("Buffered Value is Negative Infinity", func(t *testing.T) {
		data := buildValidSerializedSketch(1, 1) // 1 centroid, 1 buffered
		// Set buffered value to -Inf (offset 48-55, after centroid)
		binary.LittleEndian.PutUint64(data[48:], math.Float64bits(math.Inf(-1)))

		_, err := DecodeDouble(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "buffered value")
		assert.Contains(t, err.Error(), "Inf")
	})
}

// buildValidSerializedSketch creates a valid serialized sketch with the specified
// number of centroids and buffered values. This can then be modified to test
// invalid data handling.
func buildValidSerializedSketch(numCentroids, numBuffered uint32) []byte {
	// Calculate size: preamble(8) + numCentroids(4) + numBuffered(4) + min(8) + max(8) + centroids(16*n) + buffer(8*m)
	size := 8 + 4 + 4 + 8 + 8 + int(numCentroids)*16 + int(numBuffered)*8
	data := make([]byte, size)

	offset := 0

	// preambleLongs = 2 for multi-value sketch
	data[offset] = preambleLongsMultiple
	offset++

	// serialVersion = 1
	data[offset] = serialVersion
	offset++

	// skType = TDigest family ID
	data[offset] = uint8(internal.FamilyEnum.TDigest.Id)
	offset++

	// k = 100
	binary.LittleEndian.PutUint16(data[offset:], 100)
	offset += 2

	// flagsByte = 0 (not empty, not single value, not reverse merge)
	data[offset] = 0
	offset++

	// unused (2 bytes)
	offset += 2

	// numCentroids
	binary.LittleEndian.PutUint32(data[offset:], numCentroids)
	offset += 4

	// numBuffered
	binary.LittleEndian.PutUint32(data[offset:], numBuffered)
	offset += 4

	// min = 1.0
	binary.LittleEndian.PutUint64(data[offset:], math.Float64bits(1.0))
	offset += 8

	// max = 10.0
	binary.LittleEndian.PutUint64(data[offset:], math.Float64bits(10.0))
	offset += 8

	// centroids
	for i := uint32(0); i < numCentroids; i++ {
		// mean = 5.0
		binary.LittleEndian.PutUint64(data[offset:], math.Float64bits(5.0))
		offset += 8
		// weight = 1
		binary.LittleEndian.PutUint64(data[offset:], 1)
		offset += 8
	}

	// buffered values
	for i := uint32(0); i < numBuffered; i++ {
		// value = 5.0
		binary.LittleEndian.PutUint64(data[offset:], math.Float64bits(5.0))
		offset += 8
	}

	return data
}

func TestDecodeRejectsInvalidValuesInBothDecoders(t *testing.T) {
	compatDouble := func(minV, maxV float64, weights, means []float64) []byte {
		b := []byte{0, 0, 0, compatTypeDouble}
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(minV))
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(maxV))
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(100))
		b = binary.BigEndian.AppendUint32(b, uint32(len(weights)))
		for i := range weights {
			b = binary.BigEndian.AppendUint64(b, math.Float64bits(weights[i]))
			b = binary.BigEndian.AppendUint64(b, math.Float64bits(means[i]))
		}
		return b
	}
	compatFloat := func(minV, maxV float64, weights, means []float32) []byte {
		b := []byte{0, 0, 0, compatTypeFloat}
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(minV))
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(maxV))
		b = binary.BigEndian.AppendUint32(b, math.Float32bits(100))
		b = binary.BigEndian.AppendUint32(b, 0)
		b = binary.BigEndian.AppendUint16(b, uint16(len(weights)))
		for i := range weights {
			b = binary.BigEndian.AppendUint32(b, math.Float32bits(weights[i]))
			b = binary.BigEndian.AppendUint32(b, math.Float32bits(means[i]))
		}
		return b
	}
	decodeBoth := func(b []byte) (bytesErr, streamErr error) {
		_, bytesErr = DecodeDouble(b)
		decoder := NewDoubleDecoder()
		_, streamErr = decoder.Decode(bytes.NewReader(b))
		return bytesErr, streamErr
	}

	sketch, err := NewDouble(100)
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		assert.NoError(t, sketch.Update(float64(i)))
	}
	native, err := EncodeDouble(sketch, false)
	assert.NoError(t, err)
	withFloat := func(offset int, v float64) []byte {
		b := bytes.Clone(native)
		binary.LittleEndian.PutUint64(b[offset:], math.Float64bits(v))
		return b
	}
	const (
		minOffset          = 16
		maxOffset          = 24
		firstWeightOffset  = 40
		secondWeightOffset = 56
	)

	t.Run("valid images", func(t *testing.T) {
		for name, b := range map[string][]byte{
			"native":        native,
			"compat double": compatDouble(1, 5, []float64{1, 2}, []float64{1, 5}),
			"compat float":  compatFloat(1, 5, []float32{1, 2}, []float32{1, 5}),
		} {
			bytesErr, streamErr := decodeBoth(b)
			assert.NoError(t, bytesErr, name)
			assert.NoError(t, streamErr, name)
		}
	})

	inf, nan := math.Inf(1), math.NaN()
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"native min -Inf", withFloat(minOffset, -inf)},
		{"native min +Inf", withFloat(minOffset, inf)},
		{"native max +Inf", withFloat(maxOffset, inf)},
		{"native max NaN", withFloat(maxOffset, nan)},
		{"compat double min -Inf", compatDouble(-inf, 5, []float64{1, 2}, []float64{1, 5})},
		{"compat double max +Inf", compatDouble(1, inf, []float64{1, 2}, []float64{1, 5})},
		{"compat float min -Inf", compatFloat(-inf, 5, []float32{1, 2}, []float32{1, 5})},
		{"compat float max +Inf", compatFloat(1, inf, []float32{1, 2}, []float32{1, 5})},
		{"compat float mean NaN", compatFloat(1, 5, []float32{1, 2}, []float32{float32(nan), 5})},
		{"compat double weight NaN", compatDouble(1, 5, []float64{nan, 2}, []float64{1, 5})},
		{"compat double weight +Inf", compatDouble(1, 5, []float64{inf, 2}, []float64{1, 5})},
		{"compat double weight 0.5", compatDouble(1, 5, []float64{0.5, 2}, []float64{1, 5})},
		{"compat double weight 1.5", compatDouble(1, 5, []float64{1.5, 2}, []float64{1, 5})},
		{"compat double weight -1", compatDouble(1, 5, []float64{-1, 2}, []float64{1, 5})},
		{"compat double weight 1e30", compatDouble(1, 5, []float64{1e30, 2}, []float64{1, 5})},
		{"compat float weight NaN", compatFloat(1, 5, []float32{float32(nan), 2}, []float32{1, 5})},
		{"compat float weight +Inf", compatFloat(1, 5, []float32{float32(inf), 2}, []float32{1, 5})},
		{"compat float weight 0.5", compatFloat(1, 5, []float32{0.5, 2}, []float32{1, 5})},
		{"compat float weight 1.5", compatFloat(1, 5, []float32{1.5, 2}, []float32{1, 5})},
		{"compat float weight -1", compatFloat(1, 5, []float32{-1, 2}, []float32{1, 5})},
		{"compat double total weight overflow", compatDouble(1, 5, []float64{1 << 63, 1 << 63}, []float64{1, 5})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bytesErr, streamErr := decodeBoth(tc.data)
			assert.Error(t, bytesErr, "bytes decoder")
			assert.Error(t, streamErr, "stream decoder")
		})
	}

	t.Run("native total weight overflow", func(t *testing.T) {
		b := bytes.Clone(native)
		binary.LittleEndian.PutUint64(b[firstWeightOffset:], 1<<63)
		binary.LittleEndian.PutUint64(b[secondWeightOffset:], 1<<63)
		bytesErr, streamErr := decodeBoth(b)
		assert.Error(t, bytesErr, "bytes decoder")
		assert.Error(t, streamErr, "stream decoder")
	})

	t.Run("native total weight overflow with buffered values", func(t *testing.T) {
		b := buildValidSerializedSketch(1, 1)
		binary.LittleEndian.PutUint64(b[firstWeightOffset:], math.MaxUint64)
		bytesErr, streamErr := decodeBoth(b)
		assert.Error(t, bytesErr, "bytes decoder")
		assert.Error(t, streamErr, "stream decoder")

		binary.LittleEndian.PutUint64(b[firstWeightOffset:], math.MaxUint64-1)
		bytesErr, streamErr = decodeBoth(b)
		assert.NoError(t, bytesErr, "bytes decoder")
		assert.NoError(t, streamErr, "stream decoder")
	})
}
