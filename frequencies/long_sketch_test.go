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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFrequentItsemsStringSerialTest(t *testing.T) {
	sketch, err := NewLongSketchWithDefault(8)
	assert.NoError(t, err)
	sketch2, err := NewLongSketchWithDefault(128)
	assert.NoError(t, err)
	sketch.Update(10, 100)
	sketch.Update(10, 100)
	sketch.Update(15, 3443)
	sketch.Update(1000001, 1010230)
	sketch.Update(1000002, 1010230)

	ser, err := sketch.serializeToString()
	assert.NoError(t, err)
	newSk0, err := NewLongSketchFromString(ser)
	assert.NoError(t, err)
	newSer0, err := newSk0.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, ser, newSer0)
	assert.Equal(t, sketch.getMaximumMapCapacity(), newSk0.getMaximumMapCapacity())
	assert.Equal(t, sketch.getCurrentMapCapacity(), newSk0.getCurrentMapCapacity())

	sketch2.Update(190, 12902390)
	sketch2.Update(191, 12902390)
	sketch2.Update(192, 12902390)
	sketch2.Update(193, 12902390)
	sketch2.Update(194, 12902390)
	sketch2.Update(195, 12902390)
	sketch2.Update(196, 12902390)
	sketch2.Update(197, 12902390)
	sketch2.Update(198, 12902390)
	sketch2.Update(199, 12902390)
	sketch2.Update(200, 12902390)
	sketch2.Update(201, 12902390)
	sketch2.Update(202, 12902390)
	sketch2.Update(203, 12902390)
	sketch2.Update(204, 12902390)
	sketch2.Update(205, 12902390)
	sketch2.Update(206, 12902390)
	sketch2.Update(207, 12902390)
	sketch2.Update(208, 12902390)

	s2, err := sketch2.serializeToString()
	assert.NoError(t, err)
	newSk2, err := NewLongSketchFromString(s2)
	assert.NoError(t, err)
	newS2, err := newSk2.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, s2, newS2)
	assert.Equal(t, sketch2.getMaximumMapCapacity(), newSk2.getMaximumMapCapacity())
	assert.Equal(t, sketch2.getCurrentMapCapacity(), newSk2.getCurrentMapCapacity())
	assert.Equal(t, sketch2.getStreamLength(), newSk2.getStreamLength())

	mergedSketch, err := sketch.merge(sketch2)
	assert.NoError(t, err)
	ms, err := mergedSketch.serializeToString()
	assert.NoError(t, err)
	newMs, err := NewLongSketchFromString(ms)
	assert.NoError(t, err)
	newSMs, err := newMs.serializeToString()
	assert.NoError(t, err)
	assert.Equal(t, ms, newSMs)
	assert.Equal(t, mergedSketch.getMaximumMapCapacity(), newMs.getMaximumMapCapacity())
	assert.Equal(t, mergedSketch.getCurrentMapCapacity(), newMs.getCurrentMapCapacity())
	assert.Equal(t, mergedSketch.getStreamLength(), newMs.getStreamLength())
}
