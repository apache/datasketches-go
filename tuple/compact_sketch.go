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
	"iter"
	"slices"
	"strings"

	"github.com/apache/datasketches-go/internal/binomialbounds"
	"github.com/apache/datasketches-go/theta"
)

const (
	SerialVersionLegacy = uint8(1)
	SerialVersion       = uint8(3)
)

const (
	flagIsBigEndian = iota
	flagIsReadOnly
	flagIsEmpty
	flagIsCompact
	flagIsOrdered
)

// CompactSketch is the immutable, serializable form of a tuple sketch.
type CompactSketch[S Summary] struct {
	theta     uint64
	entries   []entry[S]
	seedHash  uint16
	isEmpty   bool
	isOrdered bool
}

// NewCompactSketch creates a new CompactSketch from any sketch implementing
// the Sketch interface.
func NewCompactSketch[S Summary](other Sketch[S], ordered bool) (*CompactSketch[S], error) {
	seedHash, err := other.SeedHash()
	if err != nil {
		return nil, err
	}

	entries := make([]entry[S], 0, other.NumRetained())
	for hash, summary := range other.All() {
		cloned, ok := summary.Clone().(S)
		if !ok {
			return nil, fmt.Errorf("cloned summary is not S")
		}

		entries = append(entries, entry[S]{
			Hash:    hash,
			Summary: cloned,
		})
	}

	isOrdered := other.IsOrdered() || ordered
	if ordered && !other.IsOrdered() {
		slices.SortFunc(entries, func(a, b entry[S]) int {
			if a.Hash < b.Hash {
				return -1
			} else if a.Hash > b.Hash {
				return 1
			}
			return 0
		})
	}

	return &CompactSketch[S]{
		theta:     other.Theta64(),
		entries:   entries,
		seedHash:  seedHash,
		isEmpty:   other.IsEmpty(),
		isOrdered: isOrdered,
	}, nil
}

// NewCompactSketchFromThetaSketch creates a new CompactSketch from
// a given theta.Sketch and a summary.
func NewCompactSketchFromThetaSketch[S Summary](
	sketch theta.Sketch, summary S, ordered bool,
) (*CompactSketch[S], error) {
	seedHash, err := sketch.SeedHash()
	if err != nil {
		return nil, err
	}

	entries := make([]entry[S], 0, sketch.NumRetained())
	for hash := range sketch.All() {
		entries = append(entries, entry[S]{
			Hash:    hash,
			Summary: summary,
		})
	}

	isOrdered := sketch.IsOrdered() || ordered
	if ordered && !sketch.IsOrdered() {
		slices.SortFunc(entries, func(a, b entry[S]) int {
			if a.Hash < b.Hash {
				return -1
			} else if a.Hash > b.Hash {
				return 1
			}
			return 0
		})
	}

	return &CompactSketch[S]{
		theta:     sketch.Theta64(),
		entries:   entries,
		seedHash:  seedHash,
		isEmpty:   sketch.IsEmpty(),
		isOrdered: isOrdered,
	}, nil
}

func newCompactSketch[S Summary](
	isEmpty, isOrdered bool, seedHash uint16, thetaVal uint64, entries []entry[S],
) *CompactSketch[S] {
	if len(entries) <= 1 {
		isOrdered = true
	}
	return &CompactSketch[S]{
		theta:     thetaVal,
		entries:   entries,
		seedHash:  seedHash,
		isEmpty:   isEmpty,
		isOrdered: isOrdered,
	}
}

// IsEstimationMode reports whether the sketch is in estimation mode,
// as opposed to exact mode.
func (s *CompactSketch[S]) IsEstimationMode() bool {
	return s.Theta64() < theta.MaxTheta && !s.IsEmpty()
}

// Theta returns theta as a fraction from 0 to 1, representing the
// effective sampling rate.
func (s *CompactSketch[S]) Theta() float64 {
	return float64(s.Theta64()) / float64(theta.MaxTheta)
}

// Estimate returns the estimated distinct count of the input stream.
func (s *CompactSketch[S]) Estimate() float64 {
	return float64(s.NumRetained()) / s.Theta()
}

// LowerBoundFromSubset returns the approximate lower error bound for
// the given number of standard deviations over a subset of retained hashes.
// numStdDevs specifies the confidence level (1, 2, or 3) corresponding to
// approximately 67%, 95%, or 99% confidence intervals.
// numSubsetEntries specifies number of items from {0, 1, ..., get_num_retained()}
// over which to estimate the bound.
func (s *CompactSketch[S]) LowerBoundFromSubset(numStdDevs uint8, numSubsetEntries uint32) (float64, error) {
	numSubsetEntries = min(numSubsetEntries, s.NumRetained())
	if !s.IsEstimationMode() {
		return float64(numSubsetEntries), nil
	}
	return binomialbounds.LowerBound(uint64(numSubsetEntries), s.Theta(), uint(numStdDevs))
}

// LowerBound returns the approximate lower error bound for the given
// number of standard deviations. numStdDevs should be 1, 2, or 3 for
// approximately 67%, 95%, or 99% confidence intervals.
func (s *CompactSketch[S]) LowerBound(numStdDevs uint8) (float64, error) {
	return s.LowerBoundFromSubset(numStdDevs, s.NumRetained())
}

// UpperBoundFromSubset returns the approximate upper error bound for
// the given number of standard deviations over a subset of retained hashes.
// numStdDevs specifies the confidence level (1, 2, or 3) corresponding to
// approximately 67%, 95%, or 99% confidence intervals.
// numSubsetEntries specifies number of items from {0, 1, ..., get_num_retained()}
// over which to estimate the bound.
func (s *CompactSketch[S]) UpperBoundFromSubset(numStdDevs uint8, numSubsetEntries uint32) (float64, error) {
	numSubsetEntries = min(numSubsetEntries, s.NumRetained())
	if !s.IsEstimationMode() {
		return float64(numSubsetEntries), nil
	}
	return binomialbounds.UpperBound(uint64(numSubsetEntries), s.Theta(), uint(numStdDevs))
}

// UpperBound returns the approximate upper error bound for the given
// number of standard deviations. numStdDevs should be 1, 2, or 3 for
// approximately 67%, 95%, or 99% confidence intervals.
func (s *CompactSketch[S]) UpperBound(numStdDevs uint8) (float64, error) {
	return s.UpperBoundFromSubset(numStdDevs, s.NumRetained())
}

// String returns a human-readable summary of this sketch.
// If printItems is true, the output includes all retained hashes.
func (s *CompactSketch[S]) String(printItems bool) string {
	seedHash, _ := s.SeedHash()
	lb, _ := s.LowerBound(2)
	ub, _ := s.UpperBound(2)

	var sb strings.Builder
	sb.WriteString("### Tuple sketch summary:\n")
	sb.WriteString(fmt.Sprintf("   num retained entries : %d\n", s.NumRetained()))
	sb.WriteString(fmt.Sprintf("   seed hash            : %d\n", seedHash))
	sb.WriteString(fmt.Sprintf("   empty?               : %t\n", s.IsEmpty()))
	sb.WriteString(fmt.Sprintf("   ordered?             : %t\n", s.IsOrdered()))
	sb.WriteString(fmt.Sprintf("   estimation mode?     : %t\n", s.IsEstimationMode()))
	sb.WriteString(fmt.Sprintf("   theta (fraction)     : %f\n", s.Theta()))
	sb.WriteString(fmt.Sprintf("   theta (raw 64-bit)   : %d\n", s.Theta64()))
	sb.WriteString(fmt.Sprintf("   estimate             : %f\n", s.Estimate()))
	sb.WriteString(fmt.Sprintf("   lower bound 95%% conf : %f\n", lb))
	sb.WriteString(fmt.Sprintf("   upper bound 95%% conf : %f\n", ub))
	sb.WriteString("### End sketch summary\n")

	if printItems {
		sb.WriteString("### Retained entries\n")
		for _, entry := range s.entries {
			sb.WriteString(fmt.Sprintf("%d: %v\n", entry.Hash, entry.Summary))
		}
		sb.WriteString("### End retained entries\n")
	}
	return sb.String()
}

// IsEmpty reports whether this sketch represents an empty set.
// Note: this is not the same as having no retained hashes.
func (s *CompactSketch[S]) IsEmpty() bool {
	return s.isEmpty
}

// IsOrdered reports whether retained hashes are sorted by hash value.
func (s *CompactSketch[S]) IsOrdered() bool {
	return s.isOrdered
}

// Theta64 returns theta as a positive integer between 0 and math.MaxUint64.
func (s *CompactSketch[S]) Theta64() uint64 {
	return s.theta
}

// NumRetained returns the number of hashes retained in the sketch.
func (s *CompactSketch[S]) NumRetained() uint32 {
	return uint32(len(s.entries))
}

// SeedHash returns the hash of the seed used to hash the input.
func (s *CompactSketch[S]) SeedHash() (uint16, error) {
	return s.seedHash, nil
}

// All returns an iterator over all hash-summary pairs in the sketch.
func (s *CompactSketch[S]) All() iter.Seq2[uint64, S] {
	return func(yield func(uint64, S) bool) {
		for _, e := range s.entries {
			if e.Hash != 0 {
				if !yield(e.Hash, e.Summary) {
					return
				}
			}
		}
	}
}

// Filter produces a new CompactSketch by applying a predicate to each entry.
// The predicate should return true for entries to keep.
func (s *CompactSketch[S]) Filter(predicate func(S) bool) (*CompactSketch[S], error) {
	return newFilteredCompactSketch(s, predicate)
}

func newFilteredCompactSketch[S Summary](sketch Sketch[S], predicate func(S) bool) (*CompactSketch[S], error) {
	seedHash, err := sketch.SeedHash()
	if err != nil {
		return nil, err
	}

	var entries []entry[S]
	for hash, summary := range sketch.All() {
		if predicate(summary) {
			cloned, ok := summary.Clone().(S)
			if !ok {
				return nil, fmt.Errorf("cloned summary is not S")
			}

			entries = append(entries, entry[S]{
				Hash:    hash,
				Summary: cloned,
			})
		}
	}

	isEmpty := !sketch.IsEstimationMode() && len(entries) == 0
	return newCompactSketch(
		isEmpty,
		sketch.IsOrdered(),
		seedHash,
		sketch.Theta64(),
		entries,
	), nil
}

func (s *CompactSketch[S]) preambleLongs() uint8 {
	if s.IsEstimationMode() {
		return 3
	}
	if s.isEmpty || len(s.entries) == 1 {
		return 1
	}
	return 2
}
