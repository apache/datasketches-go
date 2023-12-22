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

package frequencies

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestGenerateGoBinariesForCompatibilityTestingLongsSketch(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range nArr {
		sk, err := NewLongsSketchWithMaxMapSize(64)
		assert.NoError(t, err)
		for i := 1; i <= n; i++ {
			err = sk.Update(int64(i))
			assert.NoError(t, err)
		}
		if n == 0 {
			assert.True(t, sk.IsEmpty())
		} else {
			assert.False(t, sk.IsEmpty())
		}
		if n > 10 {
			assert.True(t, sk.GetMaximumError() > 0)
		} else {
			assert.Equal(t, sk.GetMaximumError(), int64(0))
		}
		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)

		slc, err := sk.ToSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/frequent_long_n%d_go.sk", internal.GoPath, n), slc, 0644)
		if err != nil {
			t.Errorf("err != nil")
		}
	}
}

func TestGoCompat(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestCrossGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestCrossGo)
	}

	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range nArr {
		bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_long_n%d_go.sk", internal.GoPath, n))
		assert.NoError(t, err)
		sketch, err := NewLongsSketchFromSlice(bytes)
		if err != nil {
			return
		}

		if n == 0 {
			assert.True(t, sketch.IsEmpty())
		} else {
			assert.False(t, sketch.IsEmpty())
		}
		if n > 10 {
			assert.True(t, sketch.GetMaximumError() > 0)
		} else {
			assert.Equal(t, sketch.GetMaximumError(), int64(0))
		}
		assert.Equal(t, sketch.GetStreamLength(), int64(n))
	}
}

func TestJavaCompat(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestCrossJava)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestCrossJava)
	}

	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range nArr {
		bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_long_n%d_java.sk", internal.JavaPath, n))
		assert.NoError(t, err)
		sketch, err := NewLongsSketchFromSlice(bytes)
		if err != nil {
			return
		}

		if n == 0 {
			assert.True(t, sketch.IsEmpty())
		} else {
			assert.False(t, sketch.IsEmpty())
		}
		if n > 10 {
			assert.True(t, sketch.GetMaximumError() > 0)
		} else {
			assert.Equal(t, sketch.GetMaximumError(), int64(0))
		}
		assert.Equal(t, sketch.GetStreamLength(), int64(n))
	}
}

func TestCppCompat(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestCrossCpp)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestCrossCpp)
	}

	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, n := range nArr {
		bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_long_n%d_cpp.sk", internal.CppPath, n))
		assert.NoError(t, err)
		sketch, err := NewLongsSketchFromSlice(bytes)
		if err != nil {
			return
		}

		if n == 0 {
			assert.True(t, sketch.IsEmpty())
		} else {
			assert.False(t, sketch.IsEmpty())
		}
		if n > 10 {
			assert.True(t, sketch.GetMaximumError() > 0)
		} else {
			assert.Equal(t, sketch.GetMaximumError(), int64(0))
		}
		assert.Equal(t, sketch.GetStreamLength(), int64(n))
	}
}
