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
	"fmt"
	"slices"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/theta"
)

// ArrayOfNumbersSketchUnion computes the union of ArrayOfNumbersSketch.
type ArrayOfNumbersSketchUnion[V Number] struct {
	policy             Policy[*ArrayOfNumbersSummary[V]]
	hashtable          *hashtable[*ArrayOfNumbersSummary[V]]
	entryLessFunc      func(a, b entry[*ArrayOfNumbersSummary[V]]) int
	theta              uint64
	numValuesInSummary uint8
}

// NewArrayOfNumbersSketchUnion creates a new union with the given options
func NewArrayOfNumbersSketchUnion[V Number](
	policy Policy[*ArrayOfNumbersSummary[V]],
	numValuesInSummary uint8,
	opts ...UnionOptionFunc,
) (*ArrayOfNumbersSketchUnion[V], error) {
	options := &unionOptions{
		lgK:  theta.DefaultLgK,
		rf:   theta.DefaultResizeFactor,
		p:    1.0,
		seed: theta.DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.lgK < theta.MinLgK {
		return nil, fmt.Errorf("lgK must not be less than %d: %d", theta.MinLgK, options.lgK)
	}
	if options.lgK > theta.MaxLgK {
		return nil, fmt.Errorf("lgK must not be greater than %d: %d", theta.MaxLgK, options.lgK)
	}
	if options.p <= 0 || options.p > 1 {
		return nil, errors.New("sampling probability must be between 0 and 1")
	}

	options.lgCurSize = startingSubMultiple(options.lgK+1, theta.MinLgK, uint8(options.rf))
	options.theta = startingThetaFromP(options.p)

	table := newHashtable[*ArrayOfNumbersSummary[V]](
		options.lgCurSize, options.lgK, options.rf, options.p, options.theta, options.seed, true,
	)

	return &ArrayOfNumbersSketchUnion[V]{
		hashtable: table,
		policy:    policy,
		entryLessFunc: func(a, b entry[*ArrayOfNumbersSummary[V]]) int {
			if a.Hash < b.Hash {
				return -1
			} else if a.Hash > b.Hash {
				return 1
			}
			return 0
		},
		theta:              table.theta,
		numValuesInSummary: numValuesInSummary,
	}, nil
}

// Update adds a sketch to the union
func (u *ArrayOfNumbersSketchUnion[V]) Update(sketch ArrayOfNumbersSketch[V]) error {
	if sketch.IsEmpty() {
		return nil
	}
	if u.numValuesInSummary != sketch.NumValuesInSummary() {
		return errors.New("numValuesInSummary does not match")
	}

	seedHash, err := internal.ComputeSeedHash(int64(u.hashtable.seed))
	if err != nil {
		return err
	}
	sketchSeedHash, err := sketch.SeedHash()
	if err != nil {
		return err
	}
	if uint16(seedHash) != sketchSeedHash {
		return errors.New("seed hash mismatch")
	}

	u.hashtable.isEmpty = false
	u.theta = min(u.theta, sketch.Theta64())

	for hash, summary := range sketch.All() {
		if hash < u.theta && hash < u.hashtable.theta {
			index, err := u.hashtable.Find(hash)
			if err != nil {
				if err == ErrKeyNotFound {
					u.hashtable.Insert(index, entry[*ArrayOfNumbersSummary[V]]{
						Hash:    hash,
						Summary: summary,
					})
					continue
				}
				return err
			}

			u.policy.Apply(u.hashtable.entries[index].Summary, summary)
		} else {
			// For ordered sketches, we can break early
			if sketch.IsOrdered() {
				break
			}
		}
	}

	u.theta = min(u.theta, u.hashtable.theta)
	return nil
}

// Result produces a copy of the current state of the Union as a compact sketch
func (u *ArrayOfNumbersSketchUnion[V]) Result(ordered bool) (*ArrayOfNumbersCompactSketch[V], error) {
	if u.hashtable.isEmpty {
		seedHash, err := internal.ComputeSeedHash(int64(u.hashtable.seed))
		if err != nil {
			return nil, err
		}
		return newArrayOfNumbersCompactSketch[V](true, true, uint16(seedHash), u.theta, nil, u.numValuesInSummary), nil
	}

	var entries []entry[*ArrayOfNumbersSummary[V]]

	thetaVal := min(u.theta, u.hashtable.theta)
	nominalNum := uint32(1 << u.hashtable.lgNomSize)

	if u.theta >= u.hashtable.theta {
		for _, e := range u.hashtable.entries {
			if e.Hash != 0 {
				entries = append(entries, e)
			}
		}
	} else {
		for _, e := range u.hashtable.entries {
			if e.Hash != 0 && e.Hash < thetaVal {
				entries = append(entries, e)
			}
		}
	}

	if uint32(len(entries)) > nominalNum {
		internal.QuickSelectFunc(
			entries, 0, len(entries)-1, int(nominalNum), u.entryLessFunc,
		)
		thetaVal = entries[nominalNum].Hash
		entries = entries[:nominalNum]
	}

	if ordered {
		slices.SortFunc(entries, u.entryLessFunc)
	}

	seedHash, err := internal.ComputeSeedHash(int64(u.hashtable.seed))
	if err != nil {
		return nil, err
	}

	return newArrayOfNumbersCompactSketch[V](u.hashtable.isEmpty, ordered, uint16(seedHash), thetaVal, entries, u.numValuesInSummary), nil
}

// OrderedResult produces a copy of the current state of the Union
// as an ordered compact sketch
func (u *ArrayOfNumbersSketchUnion[V]) OrderedResult() (*ArrayOfNumbersCompactSketch[V], error) {
	return u.Result(true)
}

// Reset resets the union to the initial empty state
func (u *ArrayOfNumbersSketchUnion[V]) Reset() {
	u.hashtable.Reset()
	u.theta = u.hashtable.theta
}

// Policy returns the policy used by this union
func (u *ArrayOfNumbersSketchUnion[V]) Policy() Policy[*ArrayOfNumbersSummary[V]] {
	return u.policy
}
