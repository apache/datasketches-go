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
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/theta"
)

func TestArrayOfStringsSketch_GenerateGoBinariesForCompatibilityTesting(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	t.Run("generate one value", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewUpdateSketch[*ArrayOfStringsSummary, []string](NewArrayOfStringsSummaryFunc)
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				s := []string{strconv.Itoa(i)}
				values := []string{"value" + strconv.Itoa(i)}
				sketch.UpdateUint64(GenerateHashKeyFromStrings(s), values)
			}

			assert.True(t, sketch.IsEmpty() == (n == 0))
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)

			compactSketch, err := sketch.Compact(false)
			assert.NoError(t, err)
			var buf bytes.Buffer
			encoder := NewEncoder[*ArrayOfStringsSummary](&buf, ArrayOfStringsSummaryWriter)
			err = encoder.Encode(compactSketch)
			assert.NoError(t, err)

			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			assert.NoError(t, err)
			err = os.WriteFile(fmt.Sprintf("%s/aos_1_n%d_go.sk", internal.GoPath, n), buf.Bytes(), 0644)
			assert.NoError(t, err)
		}
	})

	t.Run("generate three values", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewUpdateSketch[*ArrayOfStringsSummary, []string](NewArrayOfStringsSummaryFunc)
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				s := []string{strconv.Itoa(i)}
				values := []string{"a" + strconv.Itoa(i), "b" + strconv.Itoa(i), "c" + strconv.Itoa(i)}
				sketch.UpdateUint64(GenerateHashKeyFromStrings(s), values)
			}

			assert.True(t, sketch.IsEmpty() == (n == 0))
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)

			compactSketch, err := sketch.Compact(false)
			assert.NoError(t, err)
			var buf bytes.Buffer
			encoder := NewEncoder[*ArrayOfStringsSummary](&buf, ArrayOfStringsSummaryWriter)
			err = encoder.Encode(compactSketch)
			assert.NoError(t, err)

			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			assert.NoError(t, err)
			err = os.WriteFile(fmt.Sprintf("%s/aos_3_n%d_go.sk", internal.GoPath, n), buf.Bytes(), 0644)
			assert.NoError(t, err)
		}
	})

	t.Run("generate non empty no entries", func(t *testing.T) {
		sketch, err := NewUpdateSketch[*ArrayOfStringsSummary, []string](NewArrayOfStringsSummaryFunc, WithUpdateSketchP(0.01))
		assert.NoError(t, err)
		sketch.UpdateUint64(GenerateHashKeyFromStrings([]string{"key"}), []string{"value"})

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(0), sketch.NumRetained())

		compactSketch, err := sketch.Compact(false)
		assert.NoError(t, err)
		var buf bytes.Buffer
		encoder := NewEncoder[*ArrayOfStringsSummary](&buf, ArrayOfStringsSummaryWriter)
		err = encoder.Encode(compactSketch)
		assert.NoError(t, err)

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/aos_1_non_empty_no_entries_go.sk", internal.GoPath), buf.Bytes(), 0644)
		assert.NoError(t, err)
	})

	t.Run("generate multi key strings", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewUpdateSketch[*ArrayOfStringsSummary, []string](NewArrayOfStringsSummaryFunc)
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				s := []string{"key" + strconv.Itoa(i), "subkey" + strconv.Itoa(i%10)}
				values := []string{"value" + strconv.Itoa(i)}
				sketch.UpdateUint64(GenerateHashKeyFromStrings(s), values)
			}

			assert.True(t, sketch.IsEmpty() == (n == 0))
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)

			CompactSketch, err := sketch.Compact(false)
			assert.NoError(t, err)
			var buf bytes.Buffer
			encoder := NewEncoder[*ArrayOfStringsSummary](&buf, ArrayOfStringsSummaryWriter)
			err = encoder.Encode(CompactSketch)
			assert.NoError(t, err)

			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			err = os.WriteFile(fmt.Sprintf("%s/aos_multi_key_n%d_go.sk", internal.GoPath, n), buf.Bytes(), 0644)
			assert.NoError(t, err)
		}
	})

	t.Run("generate unicode strings", func(t *testing.T) {
		sketch, err := NewUpdateSketch[*ArrayOfStringsSummary, []string](NewArrayOfStringsSummaryFunc)
		assert.NoError(t, err)

		key := []string{"키", "열쇠"}
		value := []string{"밸류", "값"}
		sketch.UpdateUint64(GenerateHashKeyFromStrings(key), value)

		key = []string{"🔑", "🗝️"}
		value = []string{"📦", "🎁"}
		sketch.UpdateUint64(GenerateHashKeyFromStrings(key), value)

		key = []string{"ключ1", "ключ2"}
		value = []string{"ценить1", "ценить2"}
		sketch.UpdateUint64(GenerateHashKeyFromStrings(key), value)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(3), sketch.NumRetained())

		CompactSketch, err := sketch.Compact(false)
		assert.NoError(t, err)
		var buf bytes.Buffer
		encoder := NewEncoder[*ArrayOfStringsSummary](&buf, ArrayOfStringsSummaryWriter)
		err = encoder.Encode(CompactSketch)
		assert.NoError(t, err)

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		err = os.WriteFile(fmt.Sprintf("%s/aos_unicode_strings_go.sk", internal.GoPath), buf.Bytes(), 0644)
		assert.NoError(t, err)
	})

	t.Run("generate empty strings", func(t *testing.T) {
		sketch, err := NewUpdateSketch[*ArrayOfStringsSummary, []string](NewArrayOfStringsSummaryFunc)
		assert.NoError(t, err)

		key := []string{""}
		value := []string{"empty_key_value"}
		sketch.UpdateUint64(GenerateHashKeyFromStrings(key), value)

		key = []string{"empty_value_key"}
		value = []string{""}
		sketch.UpdateUint64(GenerateHashKeyFromStrings(key), value)

		key = []string{"", ""}
		value = []string{"", ""}
		sketch.UpdateUint64(GenerateHashKeyFromStrings(key), value)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(3), sketch.NumRetained())

		CompactSketch, err := sketch.Compact(false)
		assert.NoError(t, err)
		var buf bytes.Buffer
		encoder := NewEncoder[*ArrayOfStringsSummary](&buf, ArrayOfStringsSummaryWriter)
		err = encoder.Encode(CompactSketch)
		assert.NoError(t, err)

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		err = os.WriteFile(fmt.Sprintf("%s/aos_empty_strings_go.sk", internal.GoPath), buf.Bytes(), 0644)
		assert.NoError(t, err)
	})
}

func TestArrayOfStringsSketch_JavaCompat(t *testing.T) {
	t.Run("one value", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aos_1_n%d_java.sk", internal.JavaPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("Java file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for hash, summary := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
				assert.Len(t, summary.values, 1)
				assert.True(t, strings.HasPrefix(summary.values[0], "value"))
			}
		}
	})

	t.Run("three values", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aos_3_n%d_java.sk", internal.JavaPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("Java file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for hash, summary := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
				assert.Len(t, summary.values, 3)
				assert.True(t, strings.HasPrefix(summary.values[0], "a"))
				assert.True(t, strings.HasPrefix(summary.values[1], "b"))
				assert.True(t, strings.HasPrefix(summary.values[2], "c"))
				assert.Equal(t, summary.values[0][1:], summary.values[1][1:])
				assert.Equal(t, summary.values[0][1:], summary.values[2][1:])
			}
		}
	})

	t.Run("multi key", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aos_multikey_n%d_java.sk", internal.JavaPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("Java file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for hash, summary := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
				assert.Len(t, summary.values, 1)
				assert.True(t, strings.HasPrefix(summary.values[0], "value"))
			}
		}
	})

	t.Run("non empty no entries", func(t *testing.T) {
		filename := fmt.Sprintf("%s/aos_1_non_empty_no_entries_java.sk", internal.JavaPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Skipf("Java file not found: %s", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(0), sketch.NumRetained())
	})

	t.Run("unicode strings", func(t *testing.T) {
		filename := fmt.Sprintf("%s/aos_unicode_java.sk", internal.JavaPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Skipf("Java file not found: %s", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(3), sketch.NumRetained())

		actual := make(map[string]struct{})
		for hash, summary := range sketch.All() {
			assert.Less(t, hash, sketch.Theta64())
			actual[fmt.Sprintf("%q", summary.values)] = struct{}{}
		}

		expected := map[string]struct{}{
			fmt.Sprintf("%q", []string{"밸류", "값"}):            {},
			fmt.Sprintf("%q", []string{"📦", "🎁"}):             {},
			fmt.Sprintf("%q", []string{"ценить1", "ценить2"}): {},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("empty strings", func(t *testing.T) {
		filename := fmt.Sprintf("%s/aos_empty_strings_java.sk", internal.JavaPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Skipf("Java file not found: %s", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(3), sketch.NumRetained())

		actual := make(map[string]struct{})
		for hash, summary := range sketch.All() {
			assert.Less(t, hash, sketch.Theta64())
			actual[fmt.Sprintf("%q", summary.values)] = struct{}{}
		}

		expected := map[string]struct{}{
			fmt.Sprintf("%q", []string{"empty_key_value"}): {},
			fmt.Sprintf("%q", []string{""}):                {},
			fmt.Sprintf("%q", []string{"", ""}):            {},
		}
		assert.Equal(t, expected, actual)
	})
}

func TestArrayOfStringsSketch_CPPCompat(t *testing.T) {
	t.Run("one value", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aos_1_n%d_cpp.sk", internal.CppPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("CPP file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for hash, summary := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
				assert.Len(t, summary.values, 1)
				assert.True(t, strings.HasPrefix(summary.values[0], "value"))
			}
		}
	})

	t.Run("three values", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aos_3_n%d_cpp.sk", internal.CppPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("CPP file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for hash, summary := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
				assert.Len(t, summary.values, 3)
				assert.True(t, strings.HasPrefix(summary.values[0], "a"))
				assert.True(t, strings.HasPrefix(summary.values[1], "b"))
				assert.True(t, strings.HasPrefix(summary.values[2], "c"))
				assert.Equal(t, summary.values[0][1:], summary.values[1][1:])
				assert.Equal(t, summary.values[0][1:], summary.values[2][1:])
			}
		}
	})

	t.Run("multi key", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			filename := fmt.Sprintf("%s/aos_multikey_n%d_cpp.sk", internal.CppPath, n)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("CPP file not found: %s", filename)
				continue
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for hash, summary := range sketch.All() {
				assert.Less(t, hash, sketch.Theta64())
				assert.Len(t, summary.values, 1)
				assert.True(t, strings.HasPrefix(summary.values[0], "value"))
			}
		}
	})

	t.Run("non empty no entries", func(t *testing.T) {
		filename := fmt.Sprintf("%s/aos_1_non_empty_no_entries_cpp.sk", internal.CppPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Skipf("CPP file not found: %s", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(0), sketch.NumRetained())
	})

	t.Run("unicode strings", func(t *testing.T) {
		filename := fmt.Sprintf("%s/aos_unicode_cpp.sk", internal.CppPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Skipf("CPP file not found: %s", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(3), sketch.NumRetained())

		actual := make(map[string]struct{})
		for hash, summary := range sketch.All() {
			assert.Less(t, hash, sketch.Theta64())
			actual[fmt.Sprintf("%q", summary.values)] = struct{}{}
		}

		expected := map[string]struct{}{
			fmt.Sprintf("%q", []string{"밸류", "값"}):            {},
			fmt.Sprintf("%q", []string{"📦", "🎁"}):             {},
			fmt.Sprintf("%q", []string{"ценить1", "ценить2"}): {},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("empty strings", func(t *testing.T) {
		filename := fmt.Sprintf("%s/aos_empty_strings_cpp.sk", internal.CppPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Skipf("CPP file not found: %s", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		sketch, err := Decode[*ArrayOfStringsSummary](b, theta.DefaultSeed, ArrayOfStringsSummaryReader)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(3), sketch.NumRetained())

		actual := make(map[string]struct{})
		for hash, summary := range sketch.All() {
			assert.Less(t, hash, sketch.Theta64())
			actual[fmt.Sprintf("%q", summary.values)] = struct{}{}
		}

		expected := map[string]struct{}{
			fmt.Sprintf("%q", []string{"empty_key_value"}): {},
			fmt.Sprintf("%q", []string{""}):                {},
			fmt.Sprintf("%q", []string{"", ""}):            {},
		}
		assert.Equal(t, expected, actual)
	})
}

func TestArrayOfStringsSummaryWriter_ErrTooManyStrings(t *testing.T) {
	summary := &ArrayOfStringsSummary{
		values: make([]string, maxStringSliceLength+1),
	}

	var buf bytes.Buffer
	err := ArrayOfStringsSummaryWriter(&buf, summary)
	assert.ErrorIs(t, err, ErrTooManyStrings)
}

func TestArrayOfStringsSummaryReader_ErrTooManyStrings(t *testing.T) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.LittleEndian, uint32(0))
	assert.NoError(t, err)

	err = binary.Write(&buf, binary.LittleEndian, uint8(maxStringSliceLength+1))
	assert.NoError(t, err)

	summary, err := ArrayOfStringsSummaryReader(&buf)
	assert.Nil(t, summary)
	assert.ErrorIs(t, err, ErrTooManyStrings)
}
