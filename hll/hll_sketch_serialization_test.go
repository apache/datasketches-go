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
)

const (
	dSketchTestGenerateGo = "DSKETCH_TEST_GENERATE_GO"
	dSketchTestCrossJava  = "DSKETCH_TEST_CROSS_JAVA"
	dSketchTestCrossCpp   = "DSKETCH_TEST_CROSS_CPP"
	dSketchTestCrossGo    = "DSKETCH_TEST_CROSS_GO"
)

// Run me manually for generation
func TestGenerateGoFiles(t *testing.T) {
	if len(os.Getenv(dSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", dSketchTestGenerateGo)
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
		err = os.MkdirAll(goPath, os.ModePerm)
		assert.NoError(t, err)

		sl4, err := hll4.ToCompactSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll4_n%d_go.sk", goPath, n), sl4, 0644)
		assert.NoError(t, err)

		sl6, err := hll6.ToCompactSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll6_n%d_go.sk", goPath, n), sl6, 0644)
		assert.NoError(t, err)

		sl8, err := hll8.ToCompactSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/hll8_n%d_go.sk", goPath, n), sl8, 0644)
		assert.NoError(t, err)
	}
}

func TestJavaCompat(t *testing.T) {
	if len(os.Getenv(dSketchTestCrossJava)) == 0 {
		t.Skipf("%s not set", dSketchTestCrossJava)
	}

	t.Run("Java Hll4", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll4_n%d_java.sk", javaPath, n))
			assert.NoError(t, err)
			sketch, err := DeserializeHllSketch(bytes, true)
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
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll6_n%d_java.sk", javaPath, n))
			assert.NoError(t, err)

			sketch, err := DeserializeHllSketch(bytes, true)
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
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll8_n%d_java.sk", javaPath, n))
			assert.NoError(t, err)
			sketch, err := DeserializeHllSketch(bytes, true)
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
	if len(os.Getenv(dSketchTestCrossCpp)) == 0 {
		t.Skipf("%s not set", dSketchTestCrossCpp)
	}

	t.Run("Cpp Hll4", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll4_n%d_cpp.sk", cppPath, n))
			assert.NoError(t, err)
			sketch, err := DeserializeHllSketch(bytes, true)
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
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll6_n%d_cpp.sk", cppPath, n))
			assert.NoError(t, err)

			sketch, err := DeserializeHllSketch(bytes, true)
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
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll8_n%d_cpp.sk", cppPath, n))
			assert.NoError(t, err)
			sketch, err := DeserializeHllSketch(bytes, true)
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
	if len(os.Getenv(dSketchTestCrossGo)) == 0 {
		t.Skipf("%s not set", dSketchTestCrossGo)
	}

	t.Run("Go Hll4", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll4_n%d_go.sk", goPath, n))
			assert.NoError(t, err)

			sketch, err := DeserializeHllSketch(bytes, true)
			if err != nil {
				return
			}

			assert.Equal(t, 12, sketch.GetLgConfigK())
			est, err := sketch.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.02)
		}
	})

	t.Run("Go Hll6", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll6_n%d_go.sk", goPath, n))
			assert.NoError(t, err)

			sketch, err := DeserializeHllSketch(bytes, true)
			if err != nil {
				return
			}

			assert.Equal(t, 12, sketch.GetLgConfigK())
			est, err := sketch.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.02)
		}
	})

	t.Run("Go Hll8", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/hll8_n%d_go.sk", goPath, n))
			assert.NoError(t, err)

			sketch, err := DeserializeHllSketch(bytes, true)
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
