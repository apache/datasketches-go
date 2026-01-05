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

	"github.com/apache/datasketches-go/sampling"
	"github.com/stretchr/testify/assert"
)

func TestReservoirSamplingWithIntegers(t *testing.T) {
	// Create a reservoir sketch with k=10 (max 10 samples)
	sketch, err := sampling.NewReservoirItemsSketch[int64](10)
	assert.NoError(t, err)

	// Add 1000 items to the stream
	for i := int64(1); i <= 1000; i++ {
		sketch.Update(i)
	}

	// The sketch maintains exactly k samples
	assert.Equal(t, 10, sketch.NumSamples())
	assert.Equal(t, int64(1000), sketch.N())

	samples := sketch.Samples()
	fmt.Printf("Sampled %d integers from stream of %d\n", len(samples), sketch.N())
	fmt.Printf("Samples: %v\n", samples)
}

func TestReservoirSamplingWithStrings(t *testing.T) {
	// Generic sketch can work with any type
	sketch, err := sampling.NewReservoirItemsSketch[string](5)
	assert.NoError(t, err)

	words := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew"}
	for _, word := range words {
		sketch.Update(word)
	}

	assert.Equal(t, 5, sketch.NumSamples())
	fmt.Printf("Sampled words: %v\n", sketch.Samples())
}

func TestReservoirSamplingWithStructs(t *testing.T) {
	type LogEntry struct {
		Timestamp int64
		Message   string
	}

	sketch, err := sampling.NewReservoirItemsSketch[LogEntry](3)
	assert.NoError(t, err)

	// Simulate streaming log entries
	for i := int64(1); i <= 100; i++ {
		sketch.Update(LogEntry{
			Timestamp: i,
			Message:   fmt.Sprintf("Log message %d", i),
		})
	}

	assert.Equal(t, 3, sketch.NumSamples())
	fmt.Printf("Sampled log entries: %+v\n", sketch.Samples())
}

func TestReservoirUnion(t *testing.T) {
	// Distributed sampling: each node samples independently
	node1, _ := sampling.NewReservoirItemsSketch[int64](10)
	node2, _ := sampling.NewReservoirItemsSketch[int64](10)

	for i := int64(1); i <= 500; i++ {
		node1.Update(i)
	}
	for i := int64(501); i <= 1000; i++ {
		node2.Update(i)
	}

	// Merge samples from both nodes
	union, err := sampling.NewReservoirItemsUnion[int64](10)
	assert.NoError(t, err)

	union.UpdateSketch(node1)
	union.UpdateSketch(node2)

	result, err := union.Result()
	assert.NoError(t, err)

	assert.Equal(t, 10, result.NumSamples())
	fmt.Printf("Union result: %d samples from combined stream\n", result.NumSamples())
}
