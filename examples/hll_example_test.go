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
	"github.com/apache/datasketches-go/hll"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHllItemsSketch(t *testing.T) {
	// Create a new KLL sketch
	sketch, err := hll.NewHllSketchWithDefault()
	assert.NoError(t, err)

	// Update the sketch with 1000 items
	for i := 0; i < 1000; i++ {
		err := sketch.UpdateString(fmt.Sprintf("item_%d", i))
		assert.NoError(t, err)
	}

	// Get the estimate of the number of unique items
	estimate, err := sketch.GetEstimate()
	assert.NoError(t, err)
	assert.Greater(t, estimate, 900)
	assert.Less(t, estimate, 1100)
}
