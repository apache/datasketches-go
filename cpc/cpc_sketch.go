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

package cpc

type CpcSketch struct {
	seed int64

	//common variables
	lgK        int
	numCoupons int64 // The number of coupons collected so far.
	mergeFlag  bool  // Is the sketch the result of merging?
	fiCol      int   // First Interesting Column. This is part of a speed optimization.

	windowOffset  int
	slidingWindow []byte    //either null or size K bytes
	pairTable     pairTable //for sparse and surprising values, either null or variable size

	//The following variables are only valid in HIP varients
	kxp         float64 //used with HIP
	hipEstAccum float64 //used with HIP
}
