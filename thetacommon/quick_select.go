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

package thetacommon

func QuickSelect(arr []int64, lo int, hi int, pivot int) int64 {
	for hi > 0 {
		j := partition(arr, lo, hi)
		if j == pivot {
			return arr[pivot]
		}
		if j > pivot {
			hi = j - 1
		} else {
			lo = j + 1
		}
	}
	return arr[pivot]
}

func partition(arr []int64, lo int, hi int) int {
	i := lo      // left scan index
	j := hi + 1  // right scan index
	v := arr[lo] //partitioning item value
	for {
		// Scan right, scan left, check for scan complete, and exchange
		for arr[i] < v {
			i++
			if i == hi {
				break
			}
		}
		for v < arr[j] {
			j--
			if j == lo {
				break
			}
		}
		if i >= j {
			break
		}
		x := arr[i]
		arr[i] = arr[j]
		arr[j] = x
	}
	// put v=arr[j] into position with a[lo .. j-1] <= a[j] <= a[j+1 .. hi]
	x := arr[lo]
	arr[lo] = arr[j]
	arr[j] = x
	return j
}
