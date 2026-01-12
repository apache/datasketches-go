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

package tuple

import (
	"fmt"
	"slices"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/theta"
)

// ArrayOfNumbersSketchANotB computes the set difference of two sketches.
func ArrayOfNumbersSketchANotB[V Number](
	a, b ArrayOfNumbersSketch[V], seed uint64, ordered bool,
) (*ArrayOfNumbersCompactSketch[V], error) {
	seedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return nil, err
	}

	if a.IsEmpty() {
		return NewArrayOfNumbersCompactSketch[V](a, ordered)
	}
	if a.NumRetained() > 0 && b.IsEmpty() {
		return NewArrayOfNumbersCompactSketch[V](a, ordered)
	}

	aSeedHash, err := a.SeedHash()
	if err != nil {
		return nil, err
	}
	bSeedHash, err := b.SeedHash()
	if err != nil {
		return nil, err
	}
	if aSeedHash != uint16(seedHash) {
		return nil, fmt.Errorf("sketch A seed hash mismatch: expected %d, got %d", seedHash, aSeedHash)
	}
	if bSeedHash != uint16(seedHash) {
		return nil, fmt.Errorf("sketch B seed hash mismatch: expected %d, got %d", seedHash, bSeedHash)
	}

	thetaVal := min(a.Theta64(), b.Theta64())
	var entries []entry[*ArrayOfNumbersSummary[V]]

	if b.NumRetained() == 0 {
		for hash, summary := range a.All() {
			if hash < thetaVal {
				entries = append(entries, entry[*ArrayOfNumbersSummary[V]]{
					Hash:    hash,
					Summary: summary.Clone().(*ArrayOfNumbersSummary[V]),
				})
			}
		}
	} else if a.IsOrdered() && b.IsOrdered() {
		entries = computeSortBased(a, b, thetaVal)
	} else {
		var err error
		entries, err = computeHashBased(a, b, thetaVal)
		if err != nil {
			return nil, err
		}
	}

	isEmpty := a.IsEmpty()
	if len(entries) == 0 && thetaVal == theta.MaxTheta {
		isEmpty = true
	}

	if ordered && !a.IsOrdered() {
		slices.SortFunc(entries, func(a, b entry[*ArrayOfNumbersSummary[V]]) int {
			if a.Hash < b.Hash {
				return -1
			} else if a.Hash > b.Hash {
				return 1
			}
			return 0
		})
	}

	return newArrayOfNumbersCompactSketch[V](
		isEmpty,
		a.IsOrdered() || ordered,
		uint16(seedHash),
		thetaVal,
		entries,
		a.NumValuesInSummary(),
	), nil
}
