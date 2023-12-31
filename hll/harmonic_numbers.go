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

package hll

import "math"

const (
	numExactHarmonicNumbers = 25
	eulerMascheroniConstant = 0.577215664901532860606512090082
)

var (
	// tableOfExactHarmonicNumbers is a table of the first 25 harmonic numbers.
	tableOfExactHarmonicNumbers = []float64{
		0.0,                        // 0
		1.0,                        // 1
		1.5,                        // 2
		11.0 / 6.0,                 // 3
		25.0 / 12.0,                // 4
		137.0 / 60.0,               // 5
		49.0 / 20.0,                // 6
		363.0 / 140.0,              // 7
		761.0 / 280.0,              // 8
		7129.0 / 2520.0,            // 9
		7381.0 / 2520.0,            // 10
		83711.0 / 27720.0,          // 11
		86021.0 / 27720.0,          // 12
		1145993.0 / 360360.0,       // 13
		1171733.0 / 360360.0,       // 14
		1195757.0 / 360360.0,       // 15
		2436559.0 / 720720.0,       // 16
		42142223.0 / 12252240.0,    // 17
		14274301.0 / 4084080.0,     // 18
		275295799.0 / 77597520.0,   // 19
		55835135.0 / 15519504.0,    // 20
		18858053.0 / 5173168.0,     // 21
		19093197.0 / 5173168.0,     // 22
		444316699.0 / 118982864.0,  // 23
		1347822955.0 / 356948592.0, // 24
	}
)

// getBitMapEstimate is the estimator you would use for flat bit map random accessed, similar to a Bloom filter.
// bitVectorLength the length of the bit vector in bits. Must be > 0.
// numBitsSet the number of bits set in this bit vector. Must be >= 0 and <= bitVectorLength.
func getBitMapEstimate(bitVectorLength int, numBitsSet int) float64 {
	return float64(bitVectorLength) * (harmonicNumber(bitVectorLength) - harmonicNumber(bitVectorLength-numBitsSet))
}

// harmonicNumber returns the nth harmonic number.
func harmonicNumber(n int) float64 {
	if n < numExactHarmonicNumbers {
		return tableOfExactHarmonicNumbers[n]
	} else {
		x := float64(n)
		invSq := 1.0 / (x * x)
		sum := math.Log(x) + eulerMascheroniConstant + (1.0 / (2.0 * x))
		/* note: the number of terms included from this series expansion is appropriate
		   for the size of the exact table (25) and the precision of doubles */
		pow := invSq /* now n^-2 */
		sum -= pow * (1.0 / 12.0)
		pow *= invSq /* now n^-4 */
		sum += pow * (1.0 / 120.0)
		pow *= invSq /* now n^-6 */
		sum -= pow * (1.0 / 252.0)
		pow *= invSq /* now n^-8 */
		sum += pow * (1.0 / 240.0)
		return sum
	}
}
