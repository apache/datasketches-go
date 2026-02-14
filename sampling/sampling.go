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

// ResizeFactor controls how the internal array grows.
// Note: Go's slice append has automatic resizing, so this is kept for
// API compatibility with the Java version. Can be removed if not needed.
// TODO: In Java, this is abstracted into a common package. Consider if this should be moved to a common package in the future.
type ResizeFactor int

const (
	ResizeX1 ResizeFactor = 0
	ResizeX2 ResizeFactor = 1
	ResizeX4 ResizeFactor = 2
	ResizeX8 ResizeFactor = 3
)
