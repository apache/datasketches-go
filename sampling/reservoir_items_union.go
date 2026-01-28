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

package sampling

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"github.com/apache/datasketches-go/internal"
)

// ReservoirItemsUnion enables merging of multiple ReservoirItemsSketch instances.
// This is useful for distributed sampling where each node maintains a local sketch,
// and the results are merged to get a global sample.
//
// The union maintains statistical correctness by:
// - Dynamically choosing merge direction (lighter sketch merges into heavier)
// - Using weighted sampling with correct probability formula
// - Preserving the smaller K when merging sketches in sampling mode
type ReservoirItemsUnion[T any] struct {
	maxK   int                      // maximum k for the union
	gadget *ReservoirItemsSketch[T] // internal sketch (may be nil initially)
}

// NewReservoirItemsUnion creates a new union with the specified maximum k.
func NewReservoirItemsUnion[T any](maxK int) (*ReservoirItemsUnion[T], error) {
	if maxK < minK {
		return nil, errors.New("maxK must be at least 1")
	}

	return &ReservoirItemsUnion[T]{
		maxK:   maxK,
		gadget: nil, // Start with nil gadget, will be initialized on first update
	}, nil
}

// Update adds a single item to the union.
func (u *ReservoirItemsUnion[T]) Update(item T) {
	if u.gadget == nil {
		u.gadget, _ = NewReservoirItemsSketch[T](u.maxK)
	}
	u.gadget.Update(item)
}

// UpdateSketch merges another sketch into the union.
// This implements Java's update(ReservoirItemsSketch) with twoWayMergeInternal logic.
func (u *ReservoirItemsUnion[T]) UpdateSketch(sketch *ReservoirItemsSketch[T]) error {
	if sketch == nil || sketch.IsEmpty() {
		return nil
	}

	// Downsample if input K > maxK
	ris := sketch
	if sketch.K() > u.maxK {
		var err error
		ris, err = sketch.DownsampledCopy(u.maxK)
		if err != nil {
			return err
		}
	}

	// Initialize gadget if empty
	if u.gadget == nil || u.gadget.IsEmpty() {
		if err := u.createNewGadget(ris); err != nil {
			return err
		}
		return nil
	}

	u.twoWayMergeInternal(ris)
	return nil
}

// UpdateFromRaw creates a sketch from raw components and merges it.
// Useful in distributed environments. Items slice is used directly, not copied.
func (u *ReservoirItemsUnion[T]) UpdateFromRaw(n int64, k int, items []T) error {
	if len(items) == 0 {
		return nil
	}

	if k < minK {
		return errors.New("k must be at least 1")
	}
	if len(items) > k {
		return fmt.Errorf("items length %d exceeds k=%d", len(items), k)
	}
	if n < int64(len(items)) {
		return fmt.Errorf("items length %d cannot exceed n=%d", len(items), n)
	}

	sketch := &ReservoirItemsSketch[T]{
		k:    k,
		n:    n,
		rf:   defaultResizeFactor,
		data: items,
	}

	return u.UpdateSketch(sketch)
}

// createNewGadget initializes the gadget based on the source sketch.
// If source is in exact mode with K < maxK: upgrade to maxK.
// Otherwise: preserve source's K.
func (u *ReservoirItemsUnion[T]) createNewGadget(source *ReservoirItemsSketch[T]) error {
	if source.K() < u.maxK && source.N() <= int64(source.K()) {

		var err error
		u.gadget, err = NewReservoirItemsSketch[T](u.maxK)
		if err != nil {
			return err
		}
		u.twoWayMergeInternalStandard(source)
	} else {

		u.gadget = source.Copy()
	}
	return nil
}

// twoWayMergeInternal performs the merge based on the state of both sketches.
// This implements Java's twoWayMergeInternal logic.
func (u *ReservoirItemsUnion[T]) twoWayMergeInternal(source *ReservoirItemsSketch[T]) {
	if source.N() <= int64(source.K()) {
		// Case 1: source is in exact mode - use standard merge
		u.twoWayMergeInternalStandard(source)
	} else if u.gadget.N() < int64(u.gadget.K()) {
		// Case 2: gadget is in exact mode, source is in sampling mode
		// Swap: merge gadget into source (source becomes new gadget)
		tmp := u.gadget
		u.gadget = source.Copy()
		u.twoWayMergeInternalStandard(tmp)
	} else if source.ImplicitSampleWeight() < float64(u.gadget.N())/float64(u.gadget.K()-1) {
		// Case 3: both in sampling mode, source is "lighter"
		// Merge source into gadget
		u.twoWayMergeInternalWeighted(source)
	} else {
		// Case 4: both in sampling mode, gadget is "lighter"
		// Swap: merge gadget into source
		tmp := u.gadget
		u.gadget = source.Copy()
		u.twoWayMergeInternalWeighted(tmp)
	}
}

// twoWayMergeInternalStandard merges a sketch in exact mode (N <= K) into gadget.
// Simply updates gadget with each item from source.
func (u *ReservoirItemsUnion[T]) twoWayMergeInternalStandard(source *ReservoirItemsSketch[T]) {
	for i := 0; i < source.NumSamples(); i++ {
		u.gadget.Update(source.valueAtPosition(i))
	}
}

// twoWayMergeInternalWeighted merges a "lighter" sketch into gadget using weighted sampling.
// Uses the correct probability formula: P = (K * w) / targetTotal
func (u *ReservoirItemsUnion[T]) twoWayMergeInternalWeighted(source *ReservoirItemsSketch[T]) {
	numSourceSamples := source.K()
	sourceItemWeight := float64(source.N()) / float64(numSourceSamples)
	rescaledProb := float64(u.gadget.K()) * sourceItemWeight
	targetTotal := float64(u.gadget.N())
	tgtK := u.gadget.K()

	for i := 0; i < numSourceSamples; i++ {
		targetTotal += sourceItemWeight

		// p(keep_new_item) = (k * w) / newTotal
		rescaledFlip := targetTotal * rand.Float64()
		if rescaledFlip < rescaledProb {
			slotNo := rand.Intn(tgtK)
			u.gadget.insertValueAtPosition(source.valueAtPosition(i), slotNo)
		}
	}

	u.gadget.forceIncrementItemsSeen(source.N())
}

// Result returns a copy of the internal sketch.
func (u *ReservoirItemsUnion[T]) Result() (*ReservoirItemsSketch[T], error) {
	if u.gadget == nil {
		// Return empty sketch with maxK
		return NewReservoirItemsSketch[T](u.maxK)
	}
	return u.gadget.Copy(), nil
}

// MaxK returns the maximum k for this union.
func (u *ReservoirItemsUnion[T]) MaxK() int {
	return u.maxK
}

// Reset clears the union.
func (u *ReservoirItemsUnion[T]) Reset() {
	u.gadget = nil
}

// String returns a human-readable summary of the union.
func (u *ReservoirItemsUnion[T]) String() string {
	var sb strings.Builder
	sb.WriteString("### ReservoirItemsUnion SUMMARY:\n")
	sb.WriteString(fmt.Sprintf("   Max k: %d\n", u.maxK))
	if u.gadget == nil {
		sb.WriteString("   Gadget is nil\n")
	} else {
		sb.WriteString(fmt.Sprintf("   Gadget N: %d\n", u.gadget.N()))
		sb.WriteString(fmt.Sprintf("   Gadget K: %d\n", u.gadget.K()))
		sb.WriteString(fmt.Sprintf("   Gadget NumSamples: %d\n", u.gadget.NumSamples()))
	}
	sb.WriteString("### END UNION SUMMARY\n")
	return sb.String()
}

// Serialization constants
const (
	unionPreambleLongs = 1
	unionSerVer        = 2
	unionFlagEmpty     = 0x04
)

// ToSlice serializes the union to a byte slice.
func (u *ReservoirItemsUnion[T]) ToSlice(serde ItemsSerDe[T]) ([]byte, error) {
	empty := u.gadget == nil || u.gadget.IsEmpty()

	if empty {

		buf := make([]byte, 8)
		buf[0] = unionPreambleLongs
		buf[1] = unionSerVer
		buf[2] = byte(internal.FamilyEnum.ReservoirUnion.Id)
		buf[3] = unionFlagEmpty
		binary.LittleEndian.PutUint32(buf[4:], uint32(u.maxK))
		return buf, nil
	}

	gadgetBytes, err := u.gadget.ToSlice(serde)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 8+len(gadgetBytes))

	buf[0] = unionPreambleLongs
	buf[1] = unionSerVer
	buf[2] = byte(internal.FamilyEnum.ReservoirUnion.Id)
	buf[3] = 0
	binary.LittleEndian.PutUint32(buf[4:], uint32(u.maxK))

	copy(buf[8:], gadgetBytes)

	return buf, nil
}

// NewReservoirItemsUnionFromSlice deserializes a union from a byte slice.
func NewReservoirItemsUnionFromSlice[T any](data []byte, serde ItemsSerDe[T]) (*ReservoirItemsUnion[T], error) {
	if len(data) < 8 {
		return nil, errors.New("data too short")
	}

	preambleLongs := int(data[0] & 0x3F)
	ver := data[1]
	family := data[2]
	flags := data[3]
	maxK := int(binary.LittleEndian.Uint32(data[4:])) // uint32, not uint16

	if preambleLongs != unionPreambleLongs {
		return nil, fmt.Errorf("invalid preamble longs: expected %d, got %d", unionPreambleLongs, preambleLongs)
	}

	if ver != unionSerVer {
		if ver == 1 {
			encMaxK := binary.LittleEndian.Uint16(data[4:])
			decodedMaxK, err := decodeReservoirSize(encMaxK)
			if err != nil {
				return nil, err
			}
			maxK = decodedMaxK
		} else {
			return nil, errors.New("unsupported serialization version")
		}
	}
	if family != byte(internal.FamilyEnum.ReservoirUnion.Id) {
		return nil, errors.New("wrong sketch family")
	}

	isEmpty := (flags & unionFlagEmpty) != 0

	union, err := NewReservoirItemsUnion[T](maxK)
	if err != nil {
		return nil, err
	}

	if !isEmpty {
		if len(data) <= 8 {
			return nil, errors.New("data too short for non-empty union")
		}

		sketchData := data[8:]
		sketch, err := NewReservoirItemsSketchFromSlice[T](sketchData, serde)
		if err != nil {
			return nil, err
		}
		union.UpdateSketch(sketch)
	}

	return union, nil
}
