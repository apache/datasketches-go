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

package tuple

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/theta"
)

func TestArrayOfNumbersSketch_GenerateGoBinariesForCompatibilityTesting(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	t.Run("generate one value", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewArrayOfNumbersUpdateSketch[float64](1)
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				sketch.UpdateInt64(int64(i), []float64{float64(i)})
			}

			assert.True(t, sketch.IsEmpty() == (n == 0))
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)

			compactSketch, err := sketch.Compact(false)
			assert.NoError(t, err)
			var buf bytes.Buffer
			encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
			err = encoder.Encode(compactSketch)
			assert.NoError(t, err)

			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			assert.NoError(t, err)
			err = os.WriteFile(fmt.Sprintf("%s/aod_1_n%d_go.sk", internal.GoPath, n), buf.Bytes(), 0644)
			assert.NoError(t, err)
		}
	})

	t.Run("generate three values", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewArrayOfNumbersUpdateSketch[float64](3)
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				s := make([]float64, 0, 3)
				for j := 0; j < 3; j++ {
					s = append(s, float64(j))
				}
				sketch.UpdateInt64(int64(i), s)
			}

			assert.True(t, sketch.IsEmpty() == (n == 0))
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)

			compactSketch, err := sketch.Compact(false)
			assert.NoError(t, err)
			var buf bytes.Buffer
			encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
			err = encoder.Encode(compactSketch)
			assert.NoError(t, err)

			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			assert.NoError(t, err)
			err = os.WriteFile(fmt.Sprintf("%s/aod_3_n%d_go.sk", internal.GoPath, n), buf.Bytes(), 0644)
			assert.NoError(t, err)
		}
	})

	t.Run("generate non empty no entries", func(t *testing.T) {
		sketch, err := NewArrayOfNumbersUpdateSketch[float64](1, WithUpdateSketchP(0.01))
		assert.NoError(t, err)
		sketch.UpdateInt64(int64(1), []float64{1})

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(0), sketch.NumRetained())

		compactSketch, err := sketch.Compact(false)
		assert.NoError(t, err)
		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		err = encoder.Encode(compactSketch)
		assert.NoError(t, err)

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/aod_1_non_empty_no_entries_go.sk", internal.GoPath), buf.Bytes(), 0644)
		assert.NoError(t, err)
	})
}

func TestArrayOfNumbersSketch_JavaCompat(t *testing.T) {
	t.Run("one value", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aod_1_n%d_java.sk", internal.JavaPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("Java file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			buf := bytes.NewBuffer(b)
			sketch, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			assert.Equal(t, uint8(1), sketch.NumValuesInSummary())
			for hash := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
			}
		}
	})

	t.Run("three values", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aod_3_n%d_java.sk", internal.JavaPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("Java file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			buf := bytes.NewBuffer(b)
			sketch, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			assert.Equal(t, uint8(3), sketch.NumValuesInSummary())
			for hash, summary := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
				assert.Equal(t, summary.values[0], summary.values[1])
				assert.Equal(t, summary.values[0], summary.values[2])
			}
		}
	})

	t.Run("non empty no entries", func(t *testing.T) {
		filename := fmt.Sprintf("%s/aod_1_non_empty_no_entries_java.sk", internal.JavaPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Skipf("Java file not found: %s", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		buf := bytes.NewBuffer(b)
		sketch, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(0), sketch.NumRetained())
	})
}

func TestArrayOfNumbersSketch_CPPCompat(t *testing.T) {
	t.Run("one value", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aod_1_n%d_cpp.sk", internal.CppPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("CPP file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			buf := bytes.NewBuffer(b)
			sketch, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			assert.Equal(t, uint8(1), sketch.NumValuesInSummary())
			for hash := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
			}
		}
	})

	t.Run("three values", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aod_3_n%d_cpp.sk", internal.CppPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("CPP file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			buf := bytes.NewBuffer(b)
			sketch, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			assert.Equal(t, uint8(3), sketch.NumValuesInSummary())
			for hash, summary := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
				assert.Equal(t, summary.values[0], summary.values[1])
				assert.Equal(t, summary.values[0], summary.values[2])
			}
		}
	})

	t.Run("non empty no entries", func(t *testing.T) {
		filename := fmt.Sprintf("%s/aod_1_non_empty_no_entries_cpp.sk", internal.CppPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Skipf("CPP file not found: %s", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		buf := bytes.NewBuffer(b)
		sketch, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(0), sketch.NumRetained())
	})
}

func TestArrayOfNumbersCompactSketch_EncodeDecode(t *testing.T) {
	t.Run("Empty Sketch", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		compact, err := source.Compact(true)
		assert.NoError(t, err)

		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.NoError(t, err)

		assert.True(t, decoded.IsEmpty())
		assert.Equal(t, uint32(0), decoded.NumRetained())
	})

	t.Run("Single Entry Sketch", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		err = source.UpdateInt64(42, []float64{100.0, 200.0})
		assert.NoError(t, err)
		compact, err := source.Compact(true)
		assert.NoError(t, err)

		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, decoded.IsEmpty())
		assert.Equal(t, uint32(1), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
		assert.Equal(t, uint8(2), decoded.NumValuesInSummary())

		for _, s := range decoded.All() {
			assert.Equal(t, []float64{100.0, 200.0}, s.Values())
		}
	})

	t.Run("Multiple Entries Exact Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](3)
		assert.NoError(t, err)
		for i := 0; i < 10; i++ {
			_ = source.UpdateInt64(int64(i), []float64{float64(i * 10), float64(i * 20), float64(i * 30)})
		}
		compact, err := source.Compact(true)
		assert.NoError(t, err)

		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, decoded.IsEmpty())
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
		assert.Equal(t, compact.IsOrdered(), decoded.IsOrdered())
		assert.Equal(t, uint8(3), decoded.NumValuesInSummary())
	})

	t.Run("Large Sketch Estimation Mode", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		for i := 0; i < 8192; i++ {
			_ = source.UpdateInt64(int64(i), []float64{1.0, 2.0})
		}
		compact, err := source.Compact(true)
		assert.NoError(t, err)
		assert.True(t, compact.IsEstimationMode())

		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.NoError(t, err)

		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
		assert.Equal(t, compact.Estimate(), decoded.Estimate())
		assert.True(t, decoded.IsEstimationMode())

		expectedLB1, err := compact.LowerBound(1)
		assert.NoError(t, err)
		expectedLB2, err := compact.LowerBound(2)
		assert.NoError(t, err)
		expectedLB3, err := compact.LowerBound(3)
		assert.NoError(t, err)
		resultLB1, err := decoded.LowerBound(1)
		assert.NoError(t, err)
		resultLB2, err := decoded.LowerBound(2)
		assert.NoError(t, err)
		resultLB3, err := decoded.LowerBound(3)
		assert.NoError(t, err)
		assert.Equal(t, expectedLB1, resultLB1)
		assert.Equal(t, expectedLB2, resultLB2)
		assert.Equal(t, expectedLB3, resultLB3)

		var expectedEntries []entry[*ArrayOfNumbersSummary[float64]]
		for hash, summary := range compact.All() {
			expectedEntries = append(expectedEntries, entry[*ArrayOfNumbersSummary[float64]]{
				Hash:    hash,
				Summary: summary,
			})
		}
		var resultEntries []entry[*ArrayOfNumbersSummary[float64]]
		for hash, summary := range decoded.All() {
			resultEntries = append(resultEntries, entry[*ArrayOfNumbersSummary[float64]]{
				Hash:    hash,
				Summary: summary,
			})
		}

		assert.Equal(t, len(expectedEntries), len(resultEntries))
		for i := 0; i < len(expectedEntries); i++ {
			assert.Equal(t, expectedEntries[i].Hash, resultEntries[i].Hash)
			assert.Equal(t, expectedEntries[i].Summary, resultEntries[i].Summary)
		}
	})

	t.Run("Unordered Sketch", func(t *testing.T) {
		source, err := NewArrayOfNumbersUpdateSketch[float64](2)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, []float64{10.0, 20.0})
		_ = source.UpdateInt64(2, []float64{30.0, 40.0})
		_ = source.UpdateInt64(3, []float64{50.0, 60.0})
		compact, err := source.Compact(false)
		assert.NoError(t, err)
		assert.False(t, compact.IsOrdered())

		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, decoded.IsOrdered())
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
	})

	t.Run("Encode with custom seed, decode with wrong seed", func(t *testing.T) {
		customSeed := uint64(12345)
		wrongSeed := uint64(67890)

		sketch, err := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchSeed(customSeed))
		assert.NoError(t, err)
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{float64(i), float64(i * 2)})
		}

		compact, err := sketch.Compact(true)
		assert.NoError(t, err)

		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		_, err = DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), wrongSeed)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")
	})

	t.Run("Unordered sketch with estimation mode", func(t *testing.T) {
		sketch, _ := NewArrayOfNumbersUpdateSketch[float64](2)
		for i := 0; i < 10000; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{float64(i % 100), float64(i % 50)})
		}

		assert.True(t, sketch.IsEstimationMode())

		unordered, _ := sketch.Compact(false)
		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		_ = encoder.Encode(unordered)

		decoded, _ := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.False(t, decoded.IsOrdered())
		assert.True(t, decoded.IsEstimationMode())
	})

	t.Run("Verify summary values preserved after encode/decode", func(t *testing.T) {
		sketch, _ := NewArrayOfNumbersUpdateSketch[float64](3)
		expectedValues := make(map[uint64][]float64)
		for i := 0; i < 50; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{float64(i * 100), float64(i * 200), float64(i * 300)})
		}

		compact, _ := sketch.Compact(true)
		for hash, summary := range compact.All() {
			expectedValues[hash] = append([]float64{}, summary.Values()...)
		}

		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		_ = encoder.Encode(compact)

		decoded, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), theta.DefaultSeed)
		assert.NoError(t, err)

		for hash, summary := range decoded.All() {
			assert.Equal(t, expectedValues[hash], summary.Values())
		}
	})

	t.Run("Custom seed encode and decode", func(t *testing.T) {
		customSeed := uint64(9999)
		sketch, _ := NewArrayOfNumbersUpdateSketch[float64](2, WithUpdateSketchSeed(customSeed))
		for i := 0; i < 5000; i++ {
			_ = sketch.UpdateInt64(int64(i), []float64{float64(i), float64(i * 2)})
		}

		compact, _ := sketch.Compact(true)

		var buf bytes.Buffer
		encoder := NewArrayOfNumbersSketchEncoder[float64](&buf)
		err := encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := DecodeArrayOfNumbersCompactSketch[float64](buf.Bytes(), customSeed)
		assert.NoError(t, err)
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
	})
}

// Error helper types for testing
type arrayOfNumbersErrorWriter struct {
	err error
}

func (w *arrayOfNumbersErrorWriter) Write(p []byte) (n int, err error) {
	return 0, w.err
}

type arrayOfNumberErrorReader struct {
	err error
}

func (r *arrayOfNumberErrorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

func TestArrayOfNumbersSketchEncoderErrors(t *testing.T) {
	sketch, err := NewArrayOfNumbersUpdateSketch[float64](2)
	assert.NoError(t, err)
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateInt64(int64(i), []float64{float64(i), float64(i * 2)})
	}
	compact, err := sketch.Compact(true)
	assert.NoError(t, err)

	t.Run("Writer returns error", func(t *testing.T) {
		expectedErr := fmt.Errorf("disk full")
		errWriter := &arrayOfNumbersErrorWriter{err: expectedErr}

		encoder := NewArrayOfNumbersSketchEncoder[float64](errWriter)
		err := encoder.Encode(compact)

		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestArrayOfNumbersSketchDecoderErrors(t *testing.T) {
	t.Run("Reader returns error", func(t *testing.T) {
		expectedErr := fmt.Errorf("connection reset")
		errReader := &arrayOfNumberErrorReader{err: expectedErr}

		decoder := NewArrayOfNumbersSketchDecoderDecoder[float64](theta.DefaultSeed)
		_, err := decoder.Decode(errReader)

		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("Empty data", func(t *testing.T) {
		decoder := NewArrayOfNumbersSketchDecoderDecoder[float64](theta.DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader([]byte{}))

		assert.Error(t, err)
	})

	t.Run("Insufficient bytes", func(t *testing.T) {
		invalidData := []byte{0x01, 0x02, 0x03}

		decoder := NewArrayOfNumbersSketchDecoderDecoder[float64](theta.DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
	})

	t.Run("Invalid serial version", func(t *testing.T) {
		invalidData := make([]byte, 16)
		invalidData[0] = 1  // preamble longs
		invalidData[1] = 99 // Invalid version
		invalidData[2] = ArrayOfNumbersSketchFamily
		invalidData[3] = ArrayOfNumbersSketchType

		decoder := NewArrayOfNumbersSketchDecoderDecoder[float64](theta.DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "serial version mismatch")
	})

	t.Run("Invalid sketch family", func(t *testing.T) {
		invalidData := make([]byte, 16)
		invalidData[0] = 1                                 // preamble longs
		invalidData[1] = ArrayOfNumbersSketchSerialVersion // valid version
		invalidData[2] = 99                                // Invalid family
		invalidData[3] = ArrayOfNumbersSketchType          // valid type

		decoder := NewArrayOfNumbersSketchDecoderDecoder[float64](theta.DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
	})

	t.Run("Invalid sketch type", func(t *testing.T) {
		invalidData := make([]byte, 16)
		invalidData[0] = 1                                 // preamble longs
		invalidData[1] = ArrayOfNumbersSketchSerialVersion // valid version
		invalidData[2] = ArrayOfNumbersSketchFamily        // valid family
		invalidData[3] = 99                                // Invalid type

		decoder := NewArrayOfNumbersSketchDecoderDecoder[float64](theta.DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sketch type mismatch")
	})
}
