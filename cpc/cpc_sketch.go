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

const (
	minLgK = 4
	maxLgK = 26
)

type CpcSketch struct {
	seed int64

	//common variables
	lgK        int
	numCoupons int64 // The number of coupons collected so far.
	mergeFlag  bool  // Is the sketch the result of merging?
	fiCol      int   // First Interesting Column. This is part of a speed optimization.

	windowOffset  int
	slidingWindow []byte     //either null or size K bytes
	pairTable     *pairTable //for sparse and surprising values, either null or variable size

	//The following variables are only valid in HIP varients
	kxp         float64 //used with HIP
	hipEstAccum float64 //used with HIP
}

func NewCpcSketch(lgK int, seed int64) (*CpcSketch, error) {
	if err := checkLgK(lgK); err != nil {
		return nil, err
	}

	return &CpcSketch{
		lgK:  lgK,
		seed: seed,
		kxp:  float64(int64(1) << lgK),
	}, nil
}

func (c *CpcSketch) getFormat() cpcFormat {
	ordinal := 0
	f := c.getFlavor()
	if f == flavor_hybrid || f == flavor_sparse {
		ordinal = 2
		if !c.mergeFlag {
			ordinal |= 1
		}
	} else {
		ordinal = 0
		if c.slidingWindow != nil {
			ordinal |= 4
		}
		if c.pairTable != nil && c.pairTable.numPairs > 0 {
			ordinal |= 2
		}
		if !c.mergeFlag {
			ordinal |= 1
		}
	}
	return cpcFormat(ordinal)
}

func (c *CpcSketch) getFlavor() cpcFlavor {
	return determineFlavor(c.lgK, c.numCoupons)
}

func (c *CpcSketch) reset() {
	c.numCoupons = 0
	c.mergeFlag = false
	c.fiCol = 0
	c.windowOffset = 0
	c.slidingWindow = nil
	c.pairTable = nil
	c.kxp = float64(int64(1) << c.lgK)
	c.hipEstAccum = 0
}
