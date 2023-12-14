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

package common

// GetShortLE gets a short value from a byte array in little endian format.
func GetShortLE(array []byte, offset int) int {
	return int(array[offset]&0xFF) | (int(array[offset+1]&0xFF) << 8)
}

// PutShortLE puts a short value into a byte array in little endian format.
func PutShortLE(array []byte, offset int, value int) {
	array[offset] = byte(value)
	array[offset+1] = byte(value >> 8)
}
