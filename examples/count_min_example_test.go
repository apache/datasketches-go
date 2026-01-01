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

package examples

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/datasketches-go/count"
	"github.com/stretchr/testify/assert"
)

func TestCountMinSketch(t *testing.T) {
	seed := int64(12345)

	// Create a Count-Min Sketch with suggested parameters
	numBuckets, err := count.SuggestNumBuckets(0.1)
	assert.NoError(t, err)
	numHashes, err := count.SuggestNumHashes(0.99)
	assert.NoError(t, err)

	sketch, err := count.NewCountMinSketch(numHashes, numBuckets, seed)
	assert.NoError(t, err)

	// Update with frequency data
	for i := 0; i < 1000; i++ {
		_ = sketch.UpdateString("apple", 1)
	}
	for i := 0; i < 500; i++ {
		_ = sketch.UpdateString("banana", 1)
	}
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateString(fmt.Sprintf("item_%d", i), 1)
	}

	// Get frequency estimates (Count-Min never underestimates)
	assert.GreaterOrEqual(t, sketch.GetEstimateString("apple"), int64(1000))
	assert.GreaterOrEqual(t, sketch.GetEstimateString("banana"), int64(500))

	// Update with weight
	_ = sketch.UpdateString("grape", 50)
	assert.GreaterOrEqual(t, sketch.GetEstimateString("grape"), int64(50))

	// Create a second sketch for merging
	sketch2, err := count.NewCountMinSketch(numHashes, numBuckets, seed)
	assert.NoError(t, err)
	for i := 0; i < 500; i++ {
		_ = sketch2.UpdateString("apple", 1)
	}
	for i := 0; i < 300; i++ {
		_ = sketch2.UpdateString("orange", 1)
	}

	// Merge sketches
	err = sketch.Merge(sketch2)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, sketch.GetEstimateString("apple"), int64(1500))
	assert.GreaterOrEqual(t, sketch.GetEstimateString("orange"), int64(300))

	// Serialize and deserialize
	var buf bytes.Buffer
	err = sketch.Serialize(&buf)
	assert.NoError(t, err)

	restored, err := sketch.Deserialize(buf.Bytes(), seed)
	assert.NoError(t, err)
	assert.Equal(t, sketch.GetTotalWeight(), restored.GetTotalWeight())
}
