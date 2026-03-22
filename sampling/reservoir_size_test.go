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

package sampling

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeReservoirSize(t *testing.T) {
	testCases := []struct {
		name     string
		size     uint16
		expected int
		errMsg   string
	}{
		{
			name:     "0x0000_decodes_to_1",
			size:     0x0000,
			expected: 1,
		},
		{
			name:     "0x3800_decodes_to_128",
			size:     0x3800,
			expected: 128,
		},
		{
			name:     "0x3c80_decodes_to_200",
			size:     0x3C80,
			expected: 200,
		},
		{
			name:     "0x6001_decodes_to_4098",
			size:     0x6001,
			expected: 4098,
		},
		{
			name:     "0x61c4_decodes_to_5000",
			size:     0x61C4,
			expected: 5000,
		},
		{
			name:     "0x7435_decodes_to_25000",
			size:     0x7435,
			expected: 25000,
		},
		{
			name:     "0x83a4_decodes_to_95360",
			size:     0x83A4,
			expected: 95360,
		},
		{
			name:   "max_value",
			size:   0xFFFF,
			errMsg: "maximum valid encoded value is",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeReservoirSize(tc.size)

			if tc.errMsg != "" {
				assert.ErrorContains(t, err, tc.errMsg)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}
