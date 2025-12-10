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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/theta"
)

func int32SummaryWriter(w io.Writer, s *int32Summary) error {
	return binary.Write(w, binary.LittleEndian, s.value)
}

func int32SummaryReader(r io.Reader) (*int32Summary, error) {
	var value int32
	if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
		return nil, err
	}
	s := newInt32Summary()
	s.value = value
	return s, nil
}

func TestGenerateGoBinariesForCompatibilityTesting(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range ns {
		sketch, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		for i := 0; i < n; i++ {
			sketch.UpdateInt64(int64(i), int32(i))
		}

		assert.True(t, sketch.IsEmpty() == (n == 0))
		assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)

		compactSketch, err := sketch.Compact(false)
		assert.NoError(t, err)
		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		err = encoder.Encode(compactSketch)
		assert.NoError(t, err)

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/tuple_int_n%d_go.sk", internal.GoPath, n), buf.Bytes(), 0644)
		assert.NoError(t, err)
	}
}

func TestJavaCompat(t *testing.T) {
	ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range ns {
		b, err := os.ReadFile(fmt.Sprintf("%s/tuple_int_n%d_java.sk", internal.JavaPath, n))
		assert.NoError(t, err)

		buf := bytes.NewBuffer(b)
		sketch, err := Decode[*int32Summary](buf.Bytes(), theta.DefaultSeed, int32SummaryReader)
		assert.NoError(t, err)

		assert.Equal(t, n == 0, sketch.IsEmpty())
		assert.Equal(t, n > 1000, sketch.IsEstimationMode())
		assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
		for hash, summary := range sketch.All() {
			assert.Less(t, hash, sketch.Theta64())
			assert.Less(t, summary.value, int32(n))
		}
	}
}

func TestCPPCompat(t *testing.T) {
	ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range ns {
		b, err := os.ReadFile(fmt.Sprintf("%s/tuple_int_n%d_cpp.sk", internal.CppPath, n))
		assert.NoError(t, err)

		buf := bytes.NewBuffer(b)
		sketch, err := Decode[*int32Summary](buf.Bytes(), theta.DefaultSeed, int32SummaryReader)
		assert.NoError(t, err)

		assert.Equal(t, n == 0, sketch.IsEmpty())
		assert.Equal(t, n > 1000, sketch.IsEstimationMode())
		assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
		for hash, summary := range sketch.All() {
			assert.Less(t, hash, sketch.Theta64())
			assert.Less(t, summary.value, int32(n))
		}
	}
}

func TestCompactSketch_EncodeDecode(t *testing.T) {
	t.Run("Empty Sketch", func(t *testing.T) {
		source, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		compact, err := source.Compact(true)
		assert.NoError(t, err)

		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := Decode[*int32Summary](buf.Bytes(), theta.DefaultSeed, int32SummaryReader)
		assert.NoError(t, err)

		assert.True(t, decoded.IsEmpty())
		assert.Equal(t, uint32(0), decoded.NumRetained())
	})

	t.Run("Single Entry Sketch", func(t *testing.T) {
		source, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		err = source.UpdateInt64(42, 100)
		assert.NoError(t, err)
		compact, err := source.Compact(true)
		assert.NoError(t, err)

		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := Decode[*int32Summary](buf.Bytes(), theta.DefaultSeed, int32SummaryReader)
		assert.NoError(t, err)

		assert.False(t, decoded.IsEmpty())
		assert.Equal(t, uint32(1), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())

		for _, s := range decoded.All() {
			assert.Equal(t, int32(100), s.value)
		}
	})

	t.Run("Multiple Entries Exact Mode", func(t *testing.T) {
		source, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		for i := 0; i < 10; i++ {
			_ = source.UpdateInt64(int64(i), int32(i*10))
		}
		compact, err := source.Compact(true)
		assert.NoError(t, err)

		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := Decode[*int32Summary](buf.Bytes(), theta.DefaultSeed, int32SummaryReader)
		assert.NoError(t, err)

		assert.False(t, decoded.IsEmpty())
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
		assert.Equal(t, compact.IsOrdered(), decoded.IsOrdered())
	})

	t.Run("Large Sketch Estimation Mode", func(t *testing.T) {
		source, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			_ = source.UpdateInt64(int64(i), 1)
		}
		compact, err := source.Compact(true)
		assert.NoError(t, err)
		assert.True(t, compact.IsEstimationMode())

		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := Decode[*int32Summary](buf.Bytes(), theta.DefaultSeed, int32SummaryReader)
		assert.NoError(t, err)

		assert.True(t, decoded.IsEstimationMode())
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
	})

	t.Run("Unordered Sketch", func(t *testing.T) {
		source, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		assert.NoError(t, err)
		_ = source.UpdateInt64(1, 10)
		_ = source.UpdateInt64(2, 20)
		_ = source.UpdateInt64(3, 30)
		compact, err := source.Compact(false)
		assert.NoError(t, err)
		assert.False(t, compact.IsOrdered())

		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := Decode[*int32Summary](buf.Bytes(), theta.DefaultSeed, int32SummaryReader)
		assert.NoError(t, err)

		assert.False(t, decoded.IsOrdered())
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
	})

	t.Run("Encode with custom seed, decode with wrong seed", func(t *testing.T) {
		customSeed := uint64(12345)
		wrongSeed := uint64(67890)

		sketch, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchSeed(customSeed))
		assert.NoError(t, err)
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i), int32(i))
		}

		compact, err := sketch.Compact(true)
		assert.NoError(t, err)

		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		err = encoder.Encode(compact)
		assert.NoError(t, err)

		_, err = Decode[*int32Summary](buf.Bytes(), wrongSeed, int32SummaryReader)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")
	})

	t.Run("Unordered sketch with estimation mode", func(t *testing.T) {
		sketch, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
		for i := 0; i < 10000; i++ {
			_ = sketch.UpdateInt64(int64(i), int32(i%100))
		}

		assert.True(t, sketch.IsEstimationMode())

		unordered, _ := sketch.Compact(false)
		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		_ = encoder.Encode(unordered)

		decoded, _ := Decode[*int32Summary](buf.Bytes(), theta.DefaultSeed, int32SummaryReader)
		assert.False(t, decoded.IsOrdered())
		assert.True(t, decoded.IsEstimationMode())
	})

	t.Run("Custom seed encode and decode", func(t *testing.T) {
		customSeed := uint64(9999)
		sketch, _ := NewUpdateSketch[*int32Summary, int32](newInt32Summary, WithUpdateSketchSeed(customSeed))
		for i := 0; i < 5000; i++ {
			_ = sketch.UpdateInt64(int64(i), int32(i))
		}

		compact, _ := sketch.Compact(true)

		var buf bytes.Buffer
		encoder := NewEncoder[*int32Summary](&buf, int32SummaryWriter)
		err := encoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := Decode[*int32Summary](buf.Bytes(), customSeed, int32SummaryReader)
		assert.NoError(t, err)
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
	})
}

// Error helper types for testing
type errorWriter struct {
	err error
}

func (w *errorWriter) Write(p []byte) (n int, err error) {
	return 0, w.err
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

type partialReader struct {
	data      []byte
	readCount int
}

func (r *partialReader) Read(p []byte) (n int, err error) {
	r.readCount++
	if r.readCount == 1 {
		n = copy(p, r.data)
		return n, nil
	}
	return 0, io.ErrUnexpectedEOF
}

func TestEncoderErrors(t *testing.T) {
	sketch, err := NewUpdateSketch[*int32Summary, int32](newInt32Summary)
	assert.NoError(t, err)
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateInt64(int64(i), int32(i))
	}
	compact, err := sketch.Compact(true)
	assert.NoError(t, err)

	t.Run("Writer returns error", func(t *testing.T) {
		expectedErr := fmt.Errorf("disk full")
		errWriter := &errorWriter{err: expectedErr}

		encoder := NewEncoder[*int32Summary](errWriter, int32SummaryWriter)
		err := encoder.Encode(compact)

		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

}

func TestDecoderErrors(t *testing.T) {
	t.Run("Reader returns error", func(t *testing.T) {
		expectedErr := fmt.Errorf("connection reset")
		errReader := &errorReader{err: expectedErr}

		decoder := NewDecoder[*int32Summary](theta.DefaultSeed, int32SummaryReader)
		_, err := decoder.Decode(errReader)

		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("Reader returns unexpected EOF", func(t *testing.T) {
		errReader := &errorReader{err: io.ErrUnexpectedEOF}

		decoder := NewDecoder[*int32Summary](theta.DefaultSeed, int32SummaryReader)
		_, err := decoder.Decode(errReader)

		assert.Error(t, err)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Partial read with error", func(t *testing.T) {
		// Create some incomplete data
		partialData := []byte{0x01, 0x03, 0x09, 0x01}
		partialReader := &partialReader{data: partialData}

		decoder := NewDecoder[*int32Summary](theta.DefaultSeed, int32SummaryReader)
		_, err := decoder.Decode(partialReader)

		assert.Error(t, err)
	})

	t.Run("Empty data", func(t *testing.T) {
		decoder := NewDecoder[*int32Summary](theta.DefaultSeed, int32SummaryReader)
		_, err := decoder.Decode(bytes.NewReader([]byte{}))

		assert.Error(t, err)
	})

	t.Run("Insufficient bytes", func(t *testing.T) {
		invalidData := []byte{0x01, 0x02, 0x03}

		decoder := NewDecoder[*int32Summary](theta.DefaultSeed, int32SummaryReader)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
	})

	t.Run("Invalid serial version", func(t *testing.T) {
		invalidData := make([]byte, 8)
		invalidData[0] = 1  // preamble longs
		invalidData[1] = 99 // Invalid version
		invalidData[2] = SketchFamily
		invalidData[3] = SketchType

		decoder := NewDecoder[*int32Summary](theta.DefaultSeed, int32SummaryReader)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "serial version mismatch")
	})

	t.Run("Invalid sketch family", func(t *testing.T) {
		invalidData := make([]byte, 8)
		invalidData[0] = 1             // preamble longs
		invalidData[1] = SerialVersion // valid version
		invalidData[2] = 99            // Invalid family
		invalidData[3] = SketchType

		decoder := NewDecoder[*int32Summary](theta.DefaultSeed, int32SummaryReader)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
	})

	t.Run("Invalid sketch type", func(t *testing.T) {
		invalidData := make([]byte, 8)
		invalidData[0] = 1             // preamble longs
		invalidData[1] = SerialVersion // valid version
		invalidData[2] = SketchFamily  // valid family
		invalidData[3] = 99            // Invalid type

		decoder := NewDecoder[*int32Summary](theta.DefaultSeed, int32SummaryReader)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sketch type mismatch")
	})
}
