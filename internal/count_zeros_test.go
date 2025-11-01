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

func TestCountLeadingZerosInU64(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint64
		expected uint8
	}{
		{name: "zero", input: 0, expected: 64},
		{name: "all ones", input: 0xFFFFFFFFFFFFFFFF, expected: 0},
		{name: "one", input: 1, expected: 63},
		{name: "highest bit set", input: 0x8000000000000000, expected: 0},
		{name: "second highest bit set", input: 0x4000000000000000, expected: 1},
		{name: "byte boundary 56", input: 0x0100000000000000, expected: 7},
		{name: "byte boundary 48", input: 0x0001000000000000, expected: 15},
		{name: "byte boundary 40", input: 0x0000010000000000, expected: 23},
		{name: "byte boundary 32", input: 0x0000000100000000, expected: 31},
		{name: "byte boundary 24", input: 0x0000000001000000, expected: 39},
		{name: "byte boundary 16", input: 0x0000000000010000, expected: 47},
		{name: "byte boundary 8", input: 0x0000000000000100, expected: 55},
		{name: "lowest byte", input: 0x00000000000000FF, expected: 56},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CountLeadingZerosInU64(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCountLeadingZerosInU32(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint32
		expected uint8
	}{
		{name: "zero", input: 0, expected: 32},
		{name: "all ones", input: 0xFFFFFFFF, expected: 0},
		{name: "one", input: 1, expected: 31},
		{name: "highest bit set", input: 0x80000000, expected: 0},
		{name: "second highest bit set", input: 0x40000000, expected: 1},
		{name: "byte boundary 24", input: 0x01000000, expected: 7},
		{name: "byte boundary 16", input: 0x00010000, expected: 15},
		{name: "byte boundary 8", input: 0x00000100, expected: 23},
		{name: "lowest byte", input: 0x000000FF, expected: 24},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CountLeadingZerosInU32(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCountTrailingZerosInU64(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint64
		expected uint8
	}{
		{name: "zero", input: 0, expected: 64},
		{name: "all ones", input: 0xFFFFFFFFFFFFFFFF, expected: 0},
		{name: "one", input: 1, expected: 0},
		{name: "lowest bit set", input: 0x0000000000000001, expected: 0},
		{name: "second lowest bit set", input: 0x0000000000000002, expected: 1},
		{name: "byte boundary 8", input: 0x0000000000000100, expected: 8},
		{name: "byte boundary 16", input: 0x0000000000010000, expected: 16},
		{name: "byte boundary 24", input: 0x0000000001000000, expected: 24},
		{name: "byte boundary 32", input: 0x0000000100000000, expected: 32},
		{name: "byte boundary 40", input: 0x0000010000000000, expected: 40},
		{name: "byte boundary 48", input: 0x0001000000000000, expected: 48},
		{name: "byte boundary 56", input: 0x0100000000000000, expected: 56},
		{name: "highest bit only", input: 0x8000000000000000, expected: 63},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CountTrailingZerosInU64(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCountTrailingZerosInU32(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint32
		expected uint8
	}{
		{name: "zero", input: 0, expected: 32},
		{name: "all ones", input: 0xFFFFFFFF, expected: 0},
		{name: "one", input: 1, expected: 0},
		{name: "lowest bit set", input: 0x00000001, expected: 0},
		{name: "second lowest bit set", input: 0x00000002, expected: 1},
		{name: "byte boundary 8", input: 0x00000100, expected: 8},
		{name: "byte boundary 16", input: 0x00010000, expected: 16},
		{name: "byte boundary 24", input: 0x01000000, expected: 24},
		{name: "highest bit only", input: 0x80000000, expected: 31},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := CountTrailingZerosInU32(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
