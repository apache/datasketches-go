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

package count

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestGenerateGoSnapshots(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	t.Run("empty", func(t *testing.T) {
		sketch, err := NewCountMinSketch(1, 5, DefaultSeed)
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = sketch.Serialize(&buf)
		assert.NoError(t, err)

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/count_min_empty_go.sk", internal.GoPath), buf.Bytes(), 0644)
		assert.NoError(t, err)
	})

	t.Run("non empty", func(t *testing.T) {
		sketch, err := NewCountMinSketch(3, 1024, DefaultSeed)
		assert.NoError(t, err)
		for i := 0; i < 10; i++ {
			err := sketch.UpdateUint64(uint64(i), int64(10*i*i))
			assert.NoError(t, err)
		}

		var buf bytes.Buffer
		err = sketch.Serialize(&buf)
		assert.NoError(t, err)

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/count_min_non_empty_go.sk", internal.GoPath), buf.Bytes(), 0644)
		assert.NoError(t, err)
	})
}

func TestCPPCompact(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		filename := fmt.Sprintf("%s/count_min_empty_cpp.sk", internal.CppPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			assert.FailNowf(t, "file %s does not exist", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		sketch, err := NewCountMinSketch(1, 5, DefaultSeed)
		assert.NoError(t, err)

		result, err := sketch.Deserialize(b, DefaultSeed)
		assert.NoError(t, err)

		assert.Equal(t, sketch.GetNumHashes(), result.GetNumHashes())
		assert.Equal(t, sketch.GetNumBuckets(), result.GetNumBuckets())
		assert.Equal(t, sketch.GetSeed(), result.GetSeed())
		assert.Equal(t, sketch.GetEstimateUint64(0), result.GetEstimateUint64(0))
		assert.Equal(t, sketch.GetTotalWeight(), result.GetTotalWeight())
	})

	t.Run("non empty", func(t *testing.T) {
		filename := fmt.Sprintf("%s/count_min_non_empty_cpp.sk", internal.CppPath)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			assert.FailNowf(t, "file %s does not exist", filename)
			return
		}

		b, err := os.ReadFile(filename)
		assert.NoError(t, err)

		sketch, err := NewCountMinSketch(3, 1024, DefaultSeed)
		assert.NoError(t, err)
		for i := 0; i < 10; i++ {
			err := sketch.UpdateUint64(uint64(i), int64(10*i*i))
			assert.NoError(t, err)
		}

		result, err := sketch.Deserialize(b, DefaultSeed)
		assert.NoError(t, err)

		assert.Equal(t, sketch.GetNumHashes(), result.GetNumHashes())
		assert.Equal(t, sketch.GetNumBuckets(), result.GetNumBuckets())
		assert.Equal(t, sketch.GetSeed(), result.GetSeed())
		assert.Equal(t, sketch.GetTotalWeight(), result.GetTotalWeight())
	})
}
