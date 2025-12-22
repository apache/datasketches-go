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

package filters

import (
	"crypto/md5"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestGenerateGoBinariesForCompatibilityTesting(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	err := os.MkdirAll(internal.GoPath, os.ModePerm)
	assert.NoError(t, err)

	t.Run("bloom filter", func(t *testing.T) {
		testCases := []struct {
			n         int
			numHashes uint16
		}{
			{0, 3},
			{0, 5},
			{10000, 3},
			{10000, 5},
			{2000000, 3},
			{2000000, 5},
		}

		for _, tc := range testCases {
			// Match Java's configuration: configBits = max(n, 1000)
			configBits := uint64(tc.n)
			if configBits < 1000 {
				configBits = 1000
			}

			// Generate random seed to match Java's approach
			seed, err := GenerateRandomSeed()
			assert.NoError(t, err)

			// Create filter
			bf, err := NewBloomFilterBySize(configBits, tc.numHashes, WithSeed(seed))
			assert.NoError(t, err)

			// Insert items: n/10 items (0 to n/10-1), matching Java
			numInserts := tc.n / 10
			for i := 0; i < numInserts; i++ {
				err = bf.UpdateInt64(int64(i))
				assert.NoError(t, err)
			}

			// If non-empty, also insert NaN (matching Java)
			if tc.n > 0 {
				err = bf.UpdateFloat64(math.NaN())
				assert.NoError(t, err)
			}

			// Verify state
			assert.Equal(t, tc.n == 0, bf.IsEmpty())
			if !bf.IsEmpty() {
				assert.Greater(t, bf.BitsUsed(), uint64(numInserts))
			}

			// Serialize
			data, err := bf.ToCompactSlice()
			assert.NoError(t, err)

			// Write to file
			filename := fmt.Sprintf("%s/bf_n%d_h%d_go.sk", internal.GoPath, tc.n, tc.numHashes)
			err = os.WriteFile(filename, data, 0644)
			assert.NoError(t, err)
		}
	})

	t.Run("Specific Go", func(t *testing.T) {
		n := 10000
		numInserts := n / 10
		numHashes := uint16(3)
		configBits := uint64(n)

		// Generate for string type
		t.Run("string_type", func(t *testing.T) {
			seed, _ := GenerateRandomSeed()
			bf, _ := NewBloomFilterBySize(configBits, numHashes, WithSeed(seed))

			for i := 0; i < numInserts; i++ {
				bf.UpdateString(fmt.Sprintf("%d", i))
			}

			data, _ := bf.ToCompactSlice()
			filename := fmt.Sprintf("%s/bf_string_n%d_h%d_go.sk", internal.GoPath, n, numHashes)
			os.WriteFile(filename, data, 0644)
			t.Logf("Generated: %s", filename)
		})

		// Generate for double type
		t.Run("double_type", func(t *testing.T) {
			seed, _ := GenerateRandomSeed()
			bf, _ := NewBloomFilterBySize(configBits, numHashes, WithSeed(seed))

			for i := 0; i < numInserts; i++ {
				bf.UpdateFloat64(float64(i))
			}

			data, _ := bf.ToCompactSlice()
			filename := fmt.Sprintf("%s/bf_double_n%d_h%d_go.sk", internal.GoPath, n, numHashes)
			os.WriteFile(filename, data, 0644)
			t.Logf("Generated: %s", filename)
		})

		// Generate for long array type
		t.Run("long_array_type", func(t *testing.T) {
			seed, _ := GenerateRandomSeed()
			bf, _ := NewBloomFilterBySize(configBits, numHashes, WithSeed(seed))

			for i := 0; i < numInserts; i++ {
				arr := []int64{int64(i), int64(i)}
				bf.UpdateInt64Array(arr)
			}

			data, _ := bf.ToCompactSlice()
			filename := fmt.Sprintf("%s/bf_long_array_n%d_h%d_go.sk", internal.GoPath, n, numHashes)
			os.WriteFile(filename, data, 0644)
			t.Logf("Generated: %s", filename)
		})

		// Generate for double array type
		t.Run("double_array_type", func(t *testing.T) {
			seed, _ := GenerateRandomSeed()
			bf, _ := NewBloomFilterBySize(configBits, numHashes, WithSeed(seed))

			for i := 0; i < numInserts; i++ {
				arr := []float64{float64(i), float64(i)}
				bf.UpdateFloat64Array(arr)
			}

			data, _ := bf.ToCompactSlice()
			filename := fmt.Sprintf("%s/bf_double_array_n%d_h%d_go.sk", internal.GoPath, n, numHashes)
			os.WriteFile(filename, data, 0644)
			t.Logf("Generated: %s", filename)
		})

		// Generate for byte array type
		t.Run("byte_array_type", func(t *testing.T) {
			seed, _ := GenerateRandomSeed()
			bf, _ := NewBloomFilterBySize(configBits, numHashes, WithSeed(seed))

			for i := 0; i < numInserts; i++ {
				b := byte(i % 256)
				arr := []byte{b, b, b, b}
				bf.UpdateSlice(arr)
			}

			data, _ := bf.ToCompactSlice()
			filename := fmt.Sprintf("%s/bf_byte_array_n%d_h%d_go.sk", internal.GoPath, n, numHashes)
			os.WriteFile(filename, data, 0644)
			t.Logf("Generated: %s", filename)
		})
	})
}

func TestJavaCompat(t *testing.T) {
	t.Run("bloom filter", func(t *testing.T) {
		testCases := []struct {
			n         int
			numHashes uint16
		}{
			{0, 3},
			{0, 5},
			{10000, 3},
			{10000, 5},
			{2000000, 3},
			{2000000, 5},
		}

		for _, tc := range testCases {
			b, err := os.ReadFile(fmt.Sprintf("%s/bf_n%d_h%d_java.sk", internal.JavaPath, tc.n, tc.numHashes))
			assert.NoError(t, err)

			bf, err := NewBloomFilterFromSlice(b)
			assert.NoError(t, err)

			// Verify basic properties
			assert.Equal(t, tc.n == 0, bf.IsEmpty())
			assert.Equal(t, tc.numHashes, bf.NumHashes())

			if tc.n > 0 {
				// Verify bits used is reasonable
				assert.Greater(t, bf.BitsUsed(), uint64(0))
				assert.Less(t, bf.BitsUsed(), bf.Capacity())

				// Java inserts n/10 items (0 to n/10-1)
				numInserted := tc.n / 10

				// Verify ALL inserted items are found (no false negatives!)
				for i := 0; i < numInserted; i++ {
					assert.True(t, bf.QueryInt64(int64(i)),
						"Item %d should be found in bf_n%d_h%d_java.sk", i, tc.n, tc.numHashes)
				}

				// Verify NaN is found
				assert.True(t, bf.QueryFloat64(math.NaN()),
					"NaN should be found in bf_n%d_h%d_java.sk", tc.n, tc.numHashes)

				// Negative test: verify false positive behavior is reasonable
				// Test items that were definitely NOT inserted
				negativeTestItems := []int64{-1, -100, int64(numInserted), int64(numInserted + 1), int64(numInserted * 2)}
				foundNegativeCount := 0
				for _, item := range negativeTestItems {
					if bf.QueryInt64(item) {
						foundNegativeCount++
					}
				}

				// Should not find ALL negative items (that would indicate a bug)
				assert.Less(t, foundNegativeCount, len(negativeTestItems),
					"Should not find ALL non-inserted items (found %d/%d)", foundNegativeCount, len(negativeTestItems))

				// Test false positive rate on a larger sample
				falsePositiveCount := 0
				testRange := 1000
				startNegative := int64(numInserted + 1000)
				for i := int64(0); i < int64(testRange); i++ {
					if bf.QueryInt64(startNegative + i) {
						falsePositiveCount++
					}
				}

				fppRate := float64(falsePositiveCount) / float64(testRange)
				assert.Less(t, fppRate, 0.5,
					"False positive rate should be reasonable, got %.2f%%", fppRate*100)
			}
		}
	})
	t.Run("Specific Java", func(t *testing.T) {
		t.Skipf("Skipping Java specific coverage until we have specific type generated")
		n := 10000
		numInserts := n / 10 // 1000 items
		numHashes := uint16(3)

		testCases := []struct {
			name      string
			filename  string
			testItems func(bf BloomFilter) // Function to test items
		}{
			{
				name:     "long_type",
				filename: "bf_n10000_h3_java.sk", // Already exists - standard test
				testItems: func(bf BloomFilter) {
					// Java inserted integers 0-999
					for i := 0; i < numInserts; i++ {
						assert.True(t, bf.QueryInt64(int64(i)),
							"Should find int64 value %d", i)
					}
				},
			},
			{
				name:     "string_type",
				filename: "bf_string_n10000_h3_java.sk",
				testItems: func(bf BloomFilter) {
					// Java inserted strings "0", "1", ..., "999"
					for i := 0; i < numInserts; i++ {
						assert.True(t, bf.QueryString(fmt.Sprintf("%d", i)),
							"Should find string '%d'", i)
					}
				},
			},
			{
				name:     "double_type",
				filename: "bf_double_n10000_h3_java.sk",
				testItems: func(bf BloomFilter) {
					// Java inserted doubles 0.0, 1.0, ..., 999.0
					for i := 0; i < numInserts; i++ {
						assert.True(t, bf.QueryFloat64(float64(i)),
							"Should find double %f", float64(i))
					}
				},
			},
			{
				name:     "long_array_type",
				filename: "bf_long_array_n10000_h3_java.sk",
				testItems: func(bf BloomFilter) {
					// Java inserted long arrays [0,0], [1,1], ..., [999,999]
					for i := 0; i < numInserts; i++ {
						arr := []int64{int64(i), int64(i)}
						assert.True(t, bf.QueryInt64Array(arr),
							"Should find long array [%d,%d]", i, i)
					}
				},
			},
			{
				name:     "double_array_type",
				filename: "bf_double_array_n10000_h3_java.sk",
				testItems: func(bf BloomFilter) {
					// Java inserted double arrays [0.0,0.0], [1.0,1.0], ..., [999.0,999.0]
					for i := 0; i < numInserts; i++ {
						arr := []float64{float64(i), float64(i)}
						assert.True(t, bf.QueryFloat64Array(arr),
							"Should find double array [%f,%f]", float64(i), float64(i))
					}
				},
			},
			{
				name:     "byte_array_type",
				filename: "bf_byte_array_n10000_h3_java.sk",
				testItems: func(bf BloomFilter) {
					// Java inserted byte arrays [i,i,i,i] for i=0-999 (mod 256)
					for i := 0; i < numInserts; i++ {
						b := byte(i % 256)
						arr := []byte{b, b, b, b}
						assert.True(t, bf.QuerySlice(arr),
							"Should find byte array [%d,%d,%d,%d]", b, b, b, b)
					}
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				filepath := fmt.Sprintf("%s/%s", internal.JavaPath, tc.filename)

				// Skip if file doesn't exist
				if _, err := os.Stat(filepath); os.IsNotExist(err) {
					t.Skipf("Java file not found: %s (needs to be generated)", tc.filename)
					return
				}

				// Read Java file
				data, err := os.ReadFile(filepath)
				assert.NoError(t, err)

				// Deserialize
				bf, err := NewBloomFilterFromSlice(data)
				assert.NoError(t, err)
				assert.False(t, bf.IsEmpty())
				assert.Equal(t, numHashes, bf.NumHashes())

				// Test all items of this type
				tc.testItems(bf)

				// Compute MD5 for reference
				hash := md5.Sum(data)
				t.Logf("✅ Java %s: %d items verified, MD5=%x", tc.name, numInserts, hash)
			})
		}
	})
}

func TestCPPCompat(t *testing.T) {
	t.Run("bloom filter", func(t *testing.T) {
		testCases := []struct {
			n         int
			numHashes uint16
		}{
			{0, 3},
			{0, 5},
			{10000, 3},
			{10000, 5},
			{2000000, 3},
			{2000000, 5},
		}

		for _, tc := range testCases {
			filename := fmt.Sprintf("%s/bf_n%d_h%d_cpp.sk", internal.CppPath, tc.n, tc.numHashes)

			// Skip if file doesn't exist
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skipf("C++ file not found: %s", filename)
				return
			}

			b, err := os.ReadFile(filename)
			assert.NoError(t, err)

			bf, err := NewBloomFilterFromSlice(b)
			assert.NoError(t, err)

			// Verify basic properties
			assert.Equal(t, tc.n == 0, bf.IsEmpty())
			assert.Equal(t, tc.numHashes, bf.NumHashes())

			if tc.n > 0 {
				// Verify bits used is reasonable
				assert.Greater(t, bf.BitsUsed(), uint64(0))
				assert.Less(t, bf.BitsUsed(), bf.Capacity())

				// C++ inserts n/10 items (0 to n/10-1)
				numInserted := tc.n / 10

				// Verify ALL inserted items are found (no false negatives!)
				for i := 0; i < numInserted; i++ {
					assert.True(t, bf.QueryInt64(int64(i)),
						"Item %d should be found in bf_n%d_h%d_cpp.sk", i, tc.n, tc.numHashes)
				}

				// Verify NaN is found
				assert.True(t, bf.QueryFloat64(math.NaN()),
					"NaN should be found in bf_n%d_h%d_cpp.sk", tc.n, tc.numHashes)

				// Negative test: verify false positive behavior is reasonable
				// Test items that were definitely NOT inserted
				negativeTestItems := []int64{-1, -100, int64(numInserted), int64(numInserted + 1), int64(numInserted * 2)}
				foundNegativeCount := 0
				for _, item := range negativeTestItems {
					if bf.QueryInt64(item) {
						foundNegativeCount++
					}
				}

				// Should not find ALL negative items (that would indicate a bug)
				assert.Less(t, foundNegativeCount, len(negativeTestItems),
					"Should not find ALL non-inserted items (found %d/%d)", foundNegativeCount, len(negativeTestItems))

				// Test false positive rate on a larger sample
				falsePositiveCount := 0
				testRange := 1000
				startNegative := int64(numInserted + 1000)
				for i := int64(0); i < int64(testRange); i++ {
					if bf.QueryInt64(startNegative + i) {
						falsePositiveCount++
					}
				}

				fppRate := float64(falsePositiveCount) / float64(testRange)
				assert.Less(t, fppRate, 0.5,
					"False positive rate should be reasonable, got %.2f%%", fppRate*100)
			}
		}
	})

	t.Run("Specific Cpp", func(t *testing.T) {
		t.Skipf("Skipping C++ specific coverage until we have specific type generated")
		n := 10000
		numInserts := n / 10 // 1000 items
		numHashes := uint16(3)

		testCases := []struct {
			name      string
			filename  string
			testItems func(bf BloomFilter)
		}{
			{
				name:     "long_type",
				filename: "bf_n10000_h3_cpp.sk", // Standard test
				testItems: func(bf BloomFilter) {
					for i := 0; i < numInserts; i++ {
						assert.True(t, bf.QueryInt64(int64(i)))
					}
				},
			},
			{
				name:     "string_type",
				filename: "bf_string_n10000_h3_cpp.sk",
				testItems: func(bf BloomFilter) {
					for i := 0; i < numInserts; i++ {
						assert.True(t, bf.QueryString(fmt.Sprintf("%d", i)))
					}
				},
			},
			{
				name:     "double_type",
				filename: "bf_double_n10000_h3_cpp.sk",
				testItems: func(bf BloomFilter) {
					for i := 0; i < numInserts; i++ {
						assert.True(t, bf.QueryFloat64(float64(i)))
					}
				},
			},
			{
				name:     "byte_array_type",
				filename: "bf_byte_array_n10000_h3_cpp.sk",
				testItems: func(bf BloomFilter) {
					for i := 0; i < numInserts; i++ {
						b := byte(i % 256)
						arr := []byte{b, b, b, b}
						assert.True(t, bf.QuerySlice(arr))
					}
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				filepath := fmt.Sprintf("%s/%s", internal.CppPath, tc.filename)

				// Skip if file doesn't exist
				if _, err := os.Stat(filepath); os.IsNotExist(err) {
					t.Skipf("C++ file not found: %s (needs to be generated)", tc.filename)
					return
				}

				// Read C++ file
				data, err := os.ReadFile(filepath)
				assert.NoError(t, err)

				// Deserialize
				bf, err := NewBloomFilterFromSlice(data)
				assert.NoError(t, err)
				assert.False(t, bf.IsEmpty())
				assert.Equal(t, numHashes, bf.NumHashes())

				// Test all items
				tc.testItems(bf)

				hash := md5.Sum(data)
				t.Logf("✅ C++ %s: %d items verified, MD5=%x", tc.name, numInserts, hash)
			})
		}
	})
}
