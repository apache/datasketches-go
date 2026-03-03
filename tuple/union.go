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

// Union computes the union of Tuple sketches.
type Union[S Summary] struct {
	policy        Policy[S]
	applyFunc     func(S, S) S
	hashtable     *hashtable[S]
	entryLessFunc func(a, b entry[S]) int
	theta         uint64
}

type unionOptions struct {
	theta     uint64
	seed      uint64
	p         float32
	lgCurSize uint8
	lgK       uint8
	rf        theta.ResizeFactor
}

func (o *unionOptions) Validate() error {
	if o.lgK < theta.MinLgK {
		return fmt.Errorf("lgK must not be less than %d: %d", theta.MinLgK, o.lgK)
	}
	if o.lgK > theta.MaxLgK {
		return fmt.Errorf("lgK must not be greater than %d: %d", theta.MaxLgK, o.lgK)
	}
	if o.p <= 0 || o.p > 1 {
		return errors.New("sampling probability must be between 0 and 1")
	}

	return nil
}

type UnionOptionFunc func(*unionOptions)

// WithUnionLgK sets log2(k), where k is a nominal number of entries in the union
func WithUnionLgK(lgK uint8) UnionOptionFunc {
	return func(opts *unionOptions) {
		opts.lgK = lgK
	}
}

// WithUnionResizeFactor sets a resize factor for the internal hash table (defaults to 8)
func WithUnionResizeFactor(rf theta.ResizeFactor) UnionOptionFunc {
	return func(opts *unionOptions) {
		opts.rf = rf
	}
}

// WithUnionSketchP sets sampling probability (initial theta). The default is 1, so the union retains
// all entries until it reaches the limit, at which point it goes into the estimation mode
// and reduces the effective sampling probability (theta) as necessary
func WithUnionSketchP(p float32) UnionOptionFunc {
	return func(opts *unionOptions) {
		opts.p = p
	}
}

// WithUnionSeed sets the seed for the hash function. Should be used carefully if needed.
// Union produced with different seeds are not compatible
// and cannot be mixed in set operations.
func WithUnionSeed(seed uint64) UnionOptionFunc {
	return func(opts *unionOptions) {
		opts.seed = seed
	}
}

// NewUnion creates a new union with the given options
func NewUnion[S Summary](policy Policy[S], opts ...UnionOptionFunc) (*Union[S], error) {
	options := &unionOptions{
		lgK:  theta.DefaultLgK,
		rf:   theta.DefaultResizeFactor,
		p:    1.0,
		seed: theta.DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	if err := options.Validate(); err != nil {
		return nil, err
	}

	options.lgCurSize = startingSubMultiple(options.lgK+1, theta.MinLgK, uint8(options.rf))
	options.theta = startingThetaFromP(options.p)

	table := newHashtable[S](
		options.lgCurSize, options.lgK, options.rf, options.p, options.theta, options.seed, true,
	)

	return &Union[S]{
		hashtable: table,
		policy:    policy,
		entryLessFunc: func(a, b entry[S]) int {
			if a.Hash < b.Hash {
				return -1
			} else if a.Hash > b.Hash {
				return 1
			}
			return 0
		},
		theta: table.theta,
	}, nil
}

// NewUnionWithSummaryMergeFunc creates a new union that uses a function to merge summaries.
// This is useful for value-type summaries where Policy.Apply cannot mutate the internal summary.
func NewUnionWithSummaryMergeFunc[S Summary](
	applyFunc func(S, S) S, opts ...UnionOptionFunc,
) (*Union[S], error) {
	options := &unionOptions{
		lgK:  theta.DefaultLgK,
		rf:   theta.DefaultResizeFactor,
		p:    1.0,
		seed: theta.DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	if err := options.Validate(); err != nil {
		return nil, err
	}

	options.lgCurSize = startingSubMultiple(options.lgK+1, theta.MinLgK, uint8(options.rf))
	options.theta = startingThetaFromP(options.p)

	table := newHashtable[S](
		options.lgCurSize, options.lgK, options.rf, options.p, options.theta, options.seed, true,
	)

	return &Union[S]{
		hashtable: table,
		applyFunc: applyFunc,
		entryLessFunc: func(a, b entry[S]) int {
			if a.Hash < b.Hash {
				return -1
			} else if a.Hash > b.Hash {
				return 1
			}
			return 0
		},
		theta: table.theta,
	}, nil
}

// Update adds a sketch to the union
func (u *Union[S]) Update(sketch Sketch[S]) error {
	if sketch.IsEmpty() {
		return nil
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
					u.hashtable.Insert(index, entry[S]{
						Hash:    hash,
						Summary: summary,
					})
					continue
				}
				return err
			}

			if u.applyFunc != nil {
				u.hashtable.entries[index].Summary = u.applyFunc(u.hashtable.entries[index].Summary, summary)
			} else {
				u.policy.Apply(u.hashtable.entries[index].Summary, summary)
			}
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
func (u *Union[S]) Result(ordered bool) (*CompactSketch[S], error) {
	if u.hashtable.isEmpty {
		seedHash, err := internal.ComputeSeedHash(int64(u.hashtable.seed))
		if err != nil {
			return nil, err
		}
		return newCompactSketch[S](true, true, uint16(seedHash), u.theta, nil), nil
	}

	var entries []entry[S]

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

	return newCompactSketch[S](u.hashtable.isEmpty, ordered, uint16(seedHash), thetaVal, entries), nil
}

// OrderedResult produces a copy of the current state of the Union
// as an ordered compact sketch
func (u *Union[S]) OrderedResult() (*CompactSketch[S], error) {
	return u.Result(true)
}

// Reset resets the union to the initial empty state
func (u *Union[S]) Reset() {
	u.hashtable.Reset()
	u.theta = u.hashtable.theta
}

// Policy returns the policy used by this union
func (u *Union[S]) Policy() Policy[S] {
	return u.policy
}
