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

type cpcFormat int
type cpcFlavor int

const (
	format_empty_merged               cpcFormat = 0
	format_empty_hip                  cpcFormat = 1
	format_sparse_hybrid_merged       cpcFormat = 2
	format_sparce_hybrid_hip          cpcFormat = 3
	format_pinned_sliding_merged_nosv cpcFormat = 4
	format_pinned_sliding_hip_nosv    cpcFormat = 5
	format_pinned_sliding_merged      cpcFormat = 6
	format_pinned_sliding_hip         cpcFormat = 7
)

const (
	flavor_empty   cpcFlavor = 0 //    0  == C <    1
	flavor_sparse  cpcFlavor = 1 //    1  <= C <   3K/32
	flavor_hybrid  cpcFlavor = 2 // 3K/32 <= C <   K/2
	flavor_pinned  cpcFlavor = 3 //   K/2 <= C < 27K/8  [NB: 27/8 = 3 + 3/8]
	flavor_sliding cpcFlavor = 4 // 27K/8 <= C
)

func checkLgK(lgK int) error {
	if lgK < minLgK || lgK > maxLgK {
		return fmt.Errorf("LgK must be >= %d and <= %d: %d", minLgK, maxLgK, lgK)
	}
	return nil
}

func determineFlavor(lgK int, numCoupons int64) cpcFlavor {
	c := numCoupons
	k := int64(1) << lgK
	c2 := c << 1
	c8 := c << 3
	c32 := c << 5
	if c == 0 {
		return flavor_empty //    0  == C <    1
	}
	if c32 < (int64(3) * k) {
		return flavor_sparse //    1  <= C <   3K/32
	}
	if c2 < k {
		return flavor_hybrid // 3K/32 <= C <   K/2
	}
	if c8 < (int64(27) * k) {
		return flavor_pinned //   K/2 <= C < 27K/8
	}
	return flavor_sliding // 27K/8 <= C
}
