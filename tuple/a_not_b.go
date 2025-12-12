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

// ANotB computes the set difference of two Tuple sketches.
func ANotB[S Summary](a, b Sketch[S], seed uint64, ordered bool) (*CompactSketch[S], error) {
	seedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return nil, err
	}

	if a.IsEmpty() {
		return NewCompactSketch[S](a, ordered)
	}
	if a.NumRetained() > 0 && b.IsEmpty() {
		return NewCompactSketch[S](a, ordered)
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
	var entries []entry[S]

	if b.NumRetained() == 0 {
		for hash, summary := range a.All() {
			if hash < thetaVal {
				entries = append(entries, entry[S]{
					Hash:    hash,
					Summary: summary.Clone().(S),
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
		slices.SortFunc(entries, func(a, b entry[S]) int {
			return int(a.Hash - b.Hash)
		})
	}

	return newCompactSketch[S](
		isEmpty,
		a.IsOrdered() || ordered,
		uint16(seedHash),
		thetaVal,
		entries,
	), nil
}

func computeSortBased[S Summary](a, b Sketch[S], theta uint64) []entry[S] {
	bEntries := make(map[uint64]struct{})
	for hash := range b.All() {
		bEntries[hash] = struct{}{}
	}

	var entries []entry[S]
	for hash, summary := range a.All() {
		if _, ok := bEntries[hash]; ok {
			continue
		}

		if hash < theta {
			entries = append(entries, entry[S]{
				Hash:    hash,
				Summary: summary.Clone().(S),
			})
		}
	}
	return entries
}

func computeHashBased[S Summary](a, b Sketch[S], thetaVal uint64) ([]entry[S], error) {
	lgSize := internal.LgSizeFromCount(b.NumRetained(), rebuildThreshold)

	table := theta.NewHashtable(lgSize, lgSize, theta.ResizeX1, 1, 0, 0, false)

	for hash := range b.All() {
		if hash < thetaVal {
			idx, err := table.Find(hash)
			if err != nil && err == theta.ErrKeyNotFoundAndNoEmptySlots {
				return nil, err
			}

			table.Insert(idx, hash)
		} else if b.IsOrdered() {
			break // Early stop
		}
	}

	// Scan A and look up B
	var entries []entry[S]
	for hash, summary := range a.All() {
		if hash < thetaVal {
			_, err := table.Find(hash)
			if err != nil && err == theta.ErrKeyNotFound {
				entries = append(entries, entry[S]{
					Hash:    hash,
					Summary: summary.Clone().(S),
				})
			}
		} else if a.IsOrdered() {
			break // Early stop
		}
	}

	return entries, nil
}

// TupleANotThetaB computes the set difference of Tuple sketch from Theta sketch.
func TupleANotThetaB[S Summary](a Sketch[S], b theta.Sketch, seed uint64, ordered bool) (*CompactSketch[S], error) {
	seedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return nil, err
	}

	if a.IsEmpty() {
		return NewCompactSketch[S](a, ordered)
	}
	if a.NumRetained() > 0 && b.IsEmpty() {
		return NewCompactSketch[S](a, ordered)
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
	var entries []entry[S]

	if b.NumRetained() == 0 {
		for hash, summary := range a.All() {
			if hash < thetaVal {
				entries = append(entries, entry[S]{
					Hash:    hash,
					Summary: summary.Clone().(S),
				})
			}
		}
	} else if a.IsOrdered() && b.IsOrdered() {
		entries = computeSortBasedTupleAThetaB(a, b, thetaVal)
	} else {
		var err error
		entries, err = computeHashBasedTupleAThetaB(a, b, thetaVal)
		if err != nil {
			return nil, err
		}
	}

	isEmpty := a.IsEmpty()
	if len(entries) == 0 && thetaVal == theta.MaxTheta {
		isEmpty = true
	}

	if ordered && !a.IsOrdered() {
		slices.SortFunc(entries, func(a, b entry[S]) int {
			return int(a.Hash - b.Hash)
		})
	}

	return newCompactSketch[S](
		isEmpty,
		a.IsOrdered() || ordered,
		uint16(seedHash),
		thetaVal,
		entries,
	), nil
}

func computeSortBasedTupleAThetaB[S Summary](a Sketch[S], b theta.Sketch, theta uint64) []entry[S] {
	bEntries := make(map[uint64]struct{})
	for hash := range b.All() {
		bEntries[hash] = struct{}{}
	}

	var entries []entry[S]
	for hash, summary := range a.All() {
		if _, ok := bEntries[hash]; ok {
			continue
		}

		if hash < theta {
			entries = append(entries, entry[S]{
				Hash:    hash,
				Summary: summary.Clone().(S),
			})
		}
	}
	return entries
}

func computeHashBasedTupleAThetaB[S Summary](a Sketch[S], b theta.Sketch, thetaVal uint64) ([]entry[S], error) {
	lgSize := internal.LgSizeFromCount(b.NumRetained(), rebuildThreshold)

	table := theta.NewHashtable(lgSize, lgSize, theta.ResizeX1, 1, 0, 0, false)

	for hash := range b.All() {
		if hash < thetaVal {
			idx, err := table.Find(hash)
			if err != nil && err == theta.ErrKeyNotFoundAndNoEmptySlots {
				return nil, err
			}

			table.Insert(idx, hash)
		} else if b.IsOrdered() {
			break // Early stop
		}
	}

	// Scan A and look up B
	var entries []entry[S]
	for hash, summary := range a.All() {
		if hash < thetaVal {
			_, err := table.Find(hash)
			if err != nil && err == theta.ErrKeyNotFound {
				entries = append(entries, entry[S]{
					Hash:    hash,
					Summary: summary.Clone().(S),
				})
			}
		} else if a.IsOrdered() {
			break // Early stop
		}
	}

	return entries, nil
}
