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

import "fmt"

type CpcFormat int
type CpcFlavor int

const (
	CpcformatEmptyMerged             CpcFormat = 0
	CpcFormatEmptyHip                CpcFormat = 1
	CpcFormatSparseHybridMerged      CpcFormat = 2
	CpcFormatSparceHybridHip         CpcFormat = 3
	CpcFormatPinnedSlidingMergedNosv CpcFormat = 4
	CpcFormatPinnedSlidingHipNosv    CpcFormat = 5
	CpcFormatPinnedSlidingMerged     CpcFormat = 6
	CpcFormatPinnedSlidingHip        CpcFormat = 7
)

const (
	CpcFlavorEmpty   CpcFlavor = 0 //    0  == C <    1
	CpcFlavorSparse  CpcFlavor = 1 //    1  <= C <   3K/32
	CpcFlavorHybrid  CpcFlavor = 2 // 3K/32 <= C <   K/2
	CpcFlavorPinned  CpcFlavor = 3 //   K/2 <= C < 27K/8  [NB: 27/8 = 3 + 3/8]
	CpcFlavorSliding CpcFlavor = 4 // 27K/8 <= C
)

const (
	CpcDefaultUpdateSeed = 9001
)

func checkLgK(lgK int) error {
	if lgK < minLgK || lgK > maxLgK {
		return fmt.Errorf("LgK must be >= %d and <= %d: %d", minLgK, maxLgK, lgK)
	}
	return nil
}

func checkLgSizeInts(lgSizeInts int) error {
	if lgSizeInts < 2 || lgSizeInts > 26 {
		return fmt.Errorf("Illegal LgSizeInts: %d", lgSizeInts)
	}
	return nil
}

func determineFlavor(lgK int, numCoupons uint64) CpcFlavor {
	c := numCoupons
	k := uint64(1) << lgK
	c2 := c << 1
	c8 := c << 3
	c32 := c << 5
	if c == 0 {
		return CpcFlavorEmpty //    0  == C <    1
	}
	if c32 < (3 * k) {
		return CpcFlavorSparse //    1  <= C <   3K/32
	}
	if c2 < k {
		return CpcFlavorHybrid // 3K/32 <= C <   K/2
	}
	if c8 < (27 * k) {
		return CpcFlavorPinned //   K/2 <= C < 27K/8
	}
	return CpcFlavorSliding // 27K/8 <= C
}
