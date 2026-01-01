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
	"fmt"
	"testing"

	"github.com/apache/datasketches-go/theta"
	"github.com/stretchr/testify/assert"
)

func TestThetaSketch(t *testing.T) {
	// Create a new Theta sketch
	sketch, err := theta.NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	// Update the sketch with 1000 items
	for i := 0; i < 1000; i++ {
		_ = sketch.UpdateString(fmt.Sprintf("item_%d", i))
	}

	// Get the estimate of the number of unique items
	estimate := sketch.Estimate()
	assert.InDelta(t, 1000, estimate, 1000*0.05)

	// Create a second sketch with overlapping items
	sketch2, err := theta.NewQuickSelectUpdateSketch(theta.WithUpdateSketchLgK(14))
	assert.NoError(t, err)

	for i := 500; i < 1500; i++ {
		_ = sketch2.UpdateString(fmt.Sprintf("item_%d", i))
	}

	// Convert to compact form for set operations
	compact1 := sketch.Compact(true)
	compact2 := sketch2.Compact(true)

	// Compute union of two sketches
	union, err := theta.NewUnion()
	assert.NoError(t, err)
	err = union.Update(compact1)
	assert.NoError(t, err)
	err = union.Update(compact2)
	assert.NoError(t, err)

	unionResult, err := union.OrderedResult()
	assert.NoError(t, err)
	assert.InDelta(t, 1500, unionResult.Estimate(), 1500*0.05)

	// Compute intersection of two sketches
	intersection := theta.NewIntersection()
	err = intersection.Update(compact1)
	assert.NoError(t, err)
	err = intersection.Update(compact2)
	assert.NoError(t, err)

	intersectionResult, err := intersection.OrderedResult()
	assert.NoError(t, err)
	assert.InDelta(t, 500, intersectionResult.Estimate(), 500*0.1)

	// Compute set difference (A \ B)
	aNotBResult, err := theta.ANotB(compact1, compact2, theta.DefaultSeed, true)
	assert.NoError(t, err)
	assert.InDelta(t, 500, aNotBResult.Estimate(), 500*0.1)

	// Serialize and deserialize
	bytes1, err := compact1.MarshalBinary()
	assert.NoError(t, err)

	deserialized, err := theta.WrapCompactSketch(bytes1, theta.DefaultSeed)
	assert.NoError(t, err)
	assert.InDelta(t, estimate, deserialized.Estimate(), 1)
}
