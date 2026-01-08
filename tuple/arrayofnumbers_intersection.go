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
	"errors"
	"slices"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/theta"
)

// ArrayOfNumbersSketchIntersection computes the intersection of array of numbers sketches.
type ArrayOfNumbersSketchIntersection[V Number] struct {
	hashtable          *hashtable[*ArrayOfNumbersSummary[V]]
	policy             Policy[*ArrayOfNumbersSummary[V]]
	entryLessFunc      func(a, b entry[*ArrayOfNumbersSummary[V]]) int
	numValuesInSummary uint8
	isValid            bool
}

// NewArrayOfNumbersSketchIntersection creates a new intersection.
// NOTE: given policy should handle 'numValuesInSummary' length array in summary.
func NewArrayOfNumbersSketchIntersection[V Number](
	policy Policy[*ArrayOfNumbersSummary[V]],
	numValuesInSummary uint8,
	opts ...IntersectionOptionFunc,
) *ArrayOfNumbersSketchIntersection[V] {
	options := &intersectionOptions{
		seed: theta.DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	return &ArrayOfNumbersSketchIntersection[V]{
		hashtable: newHashtable[*ArrayOfNumbersSummary[V]](
			0, 0, theta.ResizeX1, 1.0, theta.MaxTheta, options.seed, false,
		),
		entryLessFunc: func(a, b entry[*ArrayOfNumbersSummary[V]]) int {
			if a.Hash < b.Hash {
				return -1
			} else if a.Hash > b.Hash {
				return 1
			}
			return 0
		},
		policy:             policy,
		numValuesInSummary: numValuesInSummary,
		isValid:            false,
	}
}

// Update updates the intersection with a given sketch.
func (i *ArrayOfNumbersSketchIntersection[V]) Update(sketch ArrayOfNumbersSketch[V]) error {
	if i.hashtable.isEmpty {
		return nil
	}
	if i.numValuesInSummary != sketch.NumValuesInSummary() {
		return errors.New("numValuesInSummary does not match")
	}

	seedHash, err := internal.ComputeSeedHash(int64(i.hashtable.seed))
	if err != nil {
		return err
	}
	sketchSeedHash, err := sketch.SeedHash()
	if err != nil {
		return err
	}
	if !sketch.IsEmpty() && sketchSeedHash != uint16(seedHash) {
		return errors.New("seed hash mismatch")
	}

	i.hashtable.isEmpty = i.hashtable.isEmpty || sketch.IsEmpty()
	if i.hashtable.isEmpty {
		i.hashtable.theta = theta.MaxTheta
	} else {
		i.hashtable.theta = min(i.hashtable.theta, sketch.Theta64())
	}

	if i.isValid && i.hashtable.numEntries == 0 {
		return nil
	}

	if sketch.NumRetained() == 0 {
		i.isValid = true
		i.hashtable = newHashtable[*ArrayOfNumbersSummary[V]](
			0, 0, theta.ResizeX1, 1.0, i.hashtable.theta, i.hashtable.seed, i.hashtable.isEmpty,
		)
		return nil
	}

	if !i.isValid { // first update, copy or move incoming sketch
		i.isValid = true

		lgSize := internal.LgSizeFromCount(sketch.NumRetained(), rebuildThreshold)
		i.hashtable = newHashtable[*ArrayOfNumbersSummary[V]](lgSize, lgSize-1, theta.ResizeX1, 1.0, i.hashtable.theta, i.hashtable.seed, i.hashtable.isEmpty)

		for hash, summary := range sketch.All() {
			idx, err := i.hashtable.Find(hash)
			if err == nil {
				return errors.New("duplicate key, possibly corrupted input sketch")
			}

			i.hashtable.Insert(idx, entry[*ArrayOfNumbersSummary[V]]{
				Hash:    hash,
				Summary: summary,
			})
		}

		if i.hashtable.numEntries != sketch.NumRetained() {
			return errors.New("num entries mismatch, possibly corrupted input sketch")
		}

		return nil
	}

	// intersection
	var (
		maxMatches     = min(i.hashtable.numEntries, sketch.NumRetained())
		matchesEntries = make([]entry[*ArrayOfNumbersSummary[V]], 0, maxMatches)
		matchCount     = 0
		count          = 0
	)
	for hash, summary := range sketch.All() {
		if hash < i.hashtable.theta {
			key, err := i.hashtable.Find(hash)
			if err == nil {
				if uint32(matchCount) == maxMatches {
					return errors.New("max matches exceeded, possibly corrupted input sketch")
				}

				i.policy.Apply(i.hashtable.entries[key].Summary, summary)

				matchesEntries = append(matchesEntries, i.hashtable.entries[key])
				matchCount++
			}
		} else if sketch.IsOrdered() {
			// early stop
			break
		}

		count++
	}

	if count > int(sketch.NumRetained()) {
		return errors.New("more keys than expected, possibly corrupted input sketch")
	}
	if !sketch.IsOrdered() && count < int(sketch.NumRetained()) {
		return errors.New("fewer keys than expected, possibly corrupted input sketch")
	}

	if matchCount == 0 {
		i.hashtable = newHashtable[*ArrayOfNumbersSummary[V]](
			0, 0, theta.ResizeX1, 1.0, i.hashtable.theta, i.hashtable.seed, i.hashtable.isEmpty,
		)
		if i.hashtable.theta == theta.MaxTheta {
			i.hashtable.isEmpty = true
		}
	} else {
		lgSize := internal.LgSizeFromCount(uint32(matchCount), rebuildThreshold)
		i.hashtable = newHashtable[*ArrayOfNumbersSummary[V]](
			lgSize, lgSize-1, theta.ResizeX1, 1.0, i.hashtable.theta, i.hashtable.seed, i.hashtable.isEmpty,
		)
		for j := 0; j < matchCount; j++ {
			key, err := i.hashtable.Find(matchesEntries[j].Hash)
			if err != nil && err == ErrKeyNotFoundAndNoEmptySlots {
				return err
			}

			i.hashtable.Insert(key, matchesEntries[j])
		}
	}
	return nil
}

// Result produces a copy of the current state of the intersection.
func (i *ArrayOfNumbersSketchIntersection[V]) Result(ordered bool) (*ArrayOfNumbersCompactSketch[V], error) {
	if !i.isValid {
		return nil, errors.New("calling Result() before calling Update() is undefined")
	}

	entries := make([]entry[*ArrayOfNumbersSummary[V]], 0, i.hashtable.numEntries)
	if i.hashtable.numEntries > 0 {
		for _, e := range i.hashtable.entries {
			if e.Hash != 0 {
				entries = append(entries, entry[*ArrayOfNumbersSummary[V]]{
					Hash:    e.Hash,
					Summary: e.Summary.Clone().(*ArrayOfNumbersSummary[V]),
				})
			}
		}

		if ordered {
			slices.SortFunc(entries, i.entryLessFunc)
		}
	}

	seedHash, err := internal.ComputeSeedHash(int64(i.hashtable.seed))
	if err != nil {
		return nil, err
	}

	return newArrayOfNumbersCompactSketch[V](
		i.hashtable.isEmpty,
		ordered,
		uint16(seedHash),
		i.hashtable.theta,
		entries,
		i.numValuesInSummary,
	), nil
}

// OrderedResult produces a copy of the current state of the intersection.
func (i *ArrayOfNumbersSketchIntersection[V]) OrderedResult() (*ArrayOfNumbersCompactSketch[V], error) {
	return i.Result(true)
}

// HasResult returns true if the state of the intersection is defined.
func (i *ArrayOfNumbersSketchIntersection[V]) HasResult() bool {
	return i.isValid
}

// Policy returns the policy for processing matched summary during intersection.
func (i *ArrayOfNumbersSketchIntersection[V]) Policy() Policy[*ArrayOfNumbersSummary[V]] {
	return i.policy
}
