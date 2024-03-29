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

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInvPow2(t *testing.T) {
	_, err := InvPow2(0)
	assert.NoError(t, err)
}

func TestFloorPowerOf2(t *testing.T) {
	assert.Equal(t, FloorPowerOf2(-1), int64(1))
	assert.Equal(t, FloorPowerOf2(0), int64(1))
	assert.Equal(t, FloorPowerOf2(1), int64(1))
	assert.Equal(t, FloorPowerOf2(2), int64(2))
	assert.Equal(t, FloorPowerOf2(3), int64(2))
	assert.Equal(t, FloorPowerOf2(4), int64(4))

	assert.Equal(t, FloorPowerOf2((1<<63)-1), int64(1<<62))
	assert.Equal(t, FloorPowerOf2(1<<62), int64(1<<62))
	assert.Equal(t, FloorPowerOf2((1<<62)+1), int64(1<<62))
}
