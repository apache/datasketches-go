//go:build ignore
// +build ignore

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

// This program generates serialization test data for ReservoirItemsSketch.
// Run with: go run generate_reservoir_test_data.go
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/datasketches-go/sampling"
)

func main() {
	outputDir := filepath.Join("..", "serialization_test_data", "go_generated_files")

	// Generate empty sketch: k=10, n=0
	generateEmptySketch(outputDir, 10)

	// Generate sketch with items below k: k=100, n=10
	generateSketchBelowK(outputDir, 100, 10)

	// Generate sketch at capacity: k=10, n=10
	generateSketchAtK(outputDir, 10, 10)

	// Generate sketch with sampling: k=10, n=100
	generateSketchWithSampling(outputDir, 10, 100)

	fmt.Println("All reservoir test data files generated successfully!")
}

func generateEmptySketch(dir string, k int) {
	sketch, _ := sampling.NewReservoirItemsSketch[int64](k)
	data, _ := sketch.ToByteArray(sampling.Int64SerDe{})

	filename := fmt.Sprintf("reservoir_long_n0_k%d_go.sk", k)
	writeFile(dir, filename, data)
}

func generateSketchBelowK(dir string, k, n int) {
	sketch, _ := sampling.NewReservoirItemsSketch[int64](k)
	for i := int64(1); i <= int64(n); i++ {
		sketch.Update(i)
	}
	data, _ := sketch.ToByteArray(sampling.Int64SerDe{})

	filename := fmt.Sprintf("reservoir_long_n%d_k%d_go.sk", n, k)
	writeFile(dir, filename, data)
}

func generateSketchAtK(dir string, k, n int) {
	sketch, _ := sampling.NewReservoirItemsSketch[int64](k)
	for i := int64(1); i <= int64(n); i++ {
		sketch.Update(i)
	}
	data, _ := sketch.ToByteArray(sampling.Int64SerDe{})

	filename := fmt.Sprintf("reservoir_long_n%d_k%d_go.sk", n, k)
	writeFile(dir, filename, data)
}

func generateSketchWithSampling(dir string, k, n int) {
	sketch, _ := sampling.NewReservoirItemsSketch[int64](k)
	for i := int64(1); i <= int64(n); i++ {
		sketch.Update(i)
	}
	data, _ := sketch.ToByteArray(sampling.Int64SerDe{})

	filename := fmt.Sprintf("reservoir_long_n%d_k%d_go.sk", n, k)
	writeFile(dir, filename, data)
}

func writeFile(dir, filename string, data []byte) {
	path := filepath.Join(dir, filename)
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		fmt.Printf("Error writing %s: %v\n", filename, err)
		return
	}
	fmt.Printf("Generated: %s (%d bytes)\n", filename, len(data))
}
