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
	//sketch2, err := NewLongSketchWithDefault(128)
	//assert.NoError(t, err)
	sketch.Update(10, 100)
	sketch.Update(10, 100)
	sketch.Update(15, 3443)
	sketch.Update(1000001, 1010230)
	sketch.Update(1000002, 1010230)
}
