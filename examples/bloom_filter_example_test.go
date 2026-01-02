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

	"github.com/apache/datasketches-go/filters"
	"github.com/stretchr/testify/assert"
)

func TestBloomFilter(t *testing.T) {
	// Create a Bloom filter for 1000 items with 1% false positive rate
	filter, err := filters.NewBloomFilterByAccuracy(1000, 0.01)
	assert.NoError(t, err)
	assert.True(t, filter.IsEmpty())

	// Add items to the filter
	for i := 0; i < 500; i++ {
		err := filter.UpdateString(fmt.Sprintf("user_%d", i))
		assert.NoError(t, err)
	}
	assert.False(t, filter.IsEmpty())

	// Query for items in the filter
	assert.True(t, filter.QueryString("user_0"))
	assert.True(t, filter.QueryString("user_100"))

	// Query for items not in the filter (may have false positives)
	notFoundCount := 0
	for i := 1000; i < 1100; i++ {
		if !filter.QueryString(fmt.Sprintf("user_%d", i)) {
			notFoundCount++
		}
	}
	assert.Greater(t, notFoundCount, 90)

	// Use different data types
	_ = filter.UpdateInt64(12345)
	assert.True(t, filter.QueryInt64(12345))

	// QueryAndUpdate for atomic test-and-set
	wasPresent := filter.QueryAndUpdateString("new_item")
	assert.False(t, wasPresent)
	wasPresent = filter.QueryAndUpdateString("new_item")
	assert.True(t, wasPresent)

	// Create a second filter for union
	filter2, err := filters.NewBloomFilterByAccuracy(1000, 0.01)
	assert.NoError(t, err)
	for i := 250; i < 750; i++ {
		_ = filter2.UpdateString(fmt.Sprintf("user_%d", i))
	}

	// Union two filters
	filter3, err := filters.NewBloomFilterByAccuracy(1000, 0.01)
	assert.NoError(t, err)
	for i := 0; i < 500; i++ {
		_ = filter3.UpdateString(fmt.Sprintf("user_%d", i))
	}
	err = filter3.Union(filter2)
	assert.NoError(t, err)
	assert.True(t, filter3.QueryString("user_0"))
	assert.True(t, filter3.QueryString("user_600"))

	// Serialize and deserialize
	bytes, err := filter.ToCompactSlice()
	assert.NoError(t, err)

	restored, err := filters.NewBloomFilterFromSlice(bytes)
	assert.NoError(t, err)
	assert.True(t, restored.QueryString("user_0"))
	assert.Equal(t, filter.BitsUsed(), restored.BitsUsed())
}
