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
	"testing"
	"unsafe"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/murmur3"
)

type StringHasher struct {
}

func (h StringHasher) Hash(item string) uint64 {
	datum := unsafe.Slice(unsafe.StringData(item), len(item))
	return murmur3.SeedSum64(internal.DEFAULT_UPDATE_SEED, datum[:])
}

func TestEmpty(t *testing.T) {
	h := StringHasher{}
	sketch, err := NewItemsSketchWithMaxMapSize[string](1<<_LG_MIN_MAP_SIZE, h)
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.Equal(t, sketch.GetNumActiveItems(), 0)
	assert.Equal(t, sketch.GetStreamLength(), int64(0))
	lb, err := sketch.GetLowerBound("a")
	assert.NoError(t, err)
	assert.Equal(t, lb, int64(0))
	ub, err := sketch.GetUpperBound("a")
	assert.NoError(t, err)
	assert.Equal(t, ub, int64(0))
}
