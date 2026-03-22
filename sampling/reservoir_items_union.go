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
	return &ReservoirItemsUnion[T]{
		maxK:   maxK,
		gadget: nil, // Start with nil gadget, will be initialized on first update
	}, nil
}

// Update adds a single item to the union.
//
// If the sketch contains string values and the caller cares about
// cross-language compatibility, it is the caller's responsibility to ensure
// that the input string is encoded as valid UTF-8.
func (u *ReservoirItemsUnion[T]) Update(item T) error {
	if internal.IsNil(item) {
		return nil
	}

	if u.gadget == nil {
		u.gadget, _ = NewReservoirItemsSketch[T](u.maxK)
	}

	if err := u.gadget.Update(item); err != nil {
		return err
	}
	return nil
}

// UpdateSketch merges another sketch into the union.
// This implements Java's update(ReservoirItemsSketch) with twoWayMergeInternal logic.
//
// If the sketch contains string values and the caller cares about
// cross-language compatibility, it is the caller's responsibility to ensure
// that string values in both sketches are encoded as valid UTF-8.
func (u *ReservoirItemsUnion[T]) UpdateSketch(sketch *ReservoirItemsSketch[T]) error {
	if sketch == nil {
		return nil
	}

	// Downsample if input K > maxK
	ris := sketch
	isModifiable := false
	if sketch.K() > u.maxK {
		var err error
		ris, err = sketch.DownsampledCopy(u.maxK)
		if err != nil {
			return err
		}

		// can modify the sketch if we downsampled, otherwise may need to copy it
		isModifiable = true
	}

	if u.gadget == nil {
		if err := u.createNewGadget(ris, isModifiable); err != nil {
			return err
		}
		return nil
	}

	if err := u.twoWayMergeInternal(ris, isModifiable); err != nil {
		return err
	}
	return nil
}

// UpdateFromRaw creates a sketch from raw components and merges it.
// Useful in distributed environments. Items slice is used directly, not copied.
//
// If the sketch contains string values and the caller cares about
// cross-language compatibility, it is the caller's responsibility to ensure
// that input strings are encoded as valid UTF-8.
func (u *ReservoirItemsUnion[T]) UpdateFromRaw(n int64, k int, items []T) error {
	sketch, err := newReservoirItemsSketchFromStates(
		items, n, ResizeX8, k,
	)
	if err != nil {
		return err
	}

	if sketch.K() > u.maxK {
		sketch, err = sketch.DownsampledCopy(u.maxK)
		if err != nil {
			return err
		}
	}

	if u.gadget == nil {
		return u.createNewGadget(sketch, true)
	}

	return u.twoWayMergeInternal(sketch, true)
}

// createNewGadget initializes the gadget based on the source sketch.
// If source is in exact mode with K < maxK: upgrade to maxK.
// Otherwise: preserve source's K.
func (u *ReservoirItemsUnion[T]) createNewGadget(
	source *ReservoirItemsSketch[T], isModifiable bool,
) error {
	if source.K() < u.maxK && source.N() <= int64(source.K()) {
		var err error
		u.gadget, err = NewReservoirItemsSketch[T](u.maxK)
		if err != nil {
			return err
		}
		if err := u.twoWayMergeInternal(source, isModifiable); err != nil {
			return err
		}
	} else {
		if isModifiable {
			u.gadget = source
		} else {
			u.gadget = source.Copy()
		}
	}
	return nil
}

// twoWayMergeInternal performs the merge based on the state of both sketches.
// This implements Java's twoWayMergeInternal logic.
func (u *ReservoirItemsUnion[T]) twoWayMergeInternal(
	source *ReservoirItemsSketch[T], isModifiable bool,
) error {
	if source.N() <= int64(source.K()) {
		if err := u.twoWayMergeInternalStandard(source); err != nil {
			return err
		}
	} else if u.gadget.N() < int64(u.gadget.K()) {
		// merge into sketchIn, so swap first
		tmp := u.gadget
		if isModifiable {
			u.gadget = source
		} else {
			u.gadget = source.Copy()
		}
		if err := u.twoWayMergeInternalStandard(tmp); err != nil {
			return err
		}
	} else if source.ImplicitSampleWeight() < float64(u.gadget.N())/float64(u.gadget.K()-1) { // implicit weights in sketchIn are light enough to merge into gadget
		if err := u.twoWayMergeInternalWeighted(source); err != nil {
			return err
		}
	} else {
		// implicit weights in gadget are light enough to merge into sketchIn
		// merge into sketchIn, so swap first
		tmp := u.gadget
		u.gadget = source.Copy()
		if err := u.twoWayMergeInternalWeighted(tmp); err != nil {
			return err
		}
	}

	return nil
}

// twoWayMergeInternalStandard merges a sketch in exact mode (N <= K) into gadget.
// Simply updates gadget with each item from source.
func (u *ReservoirItemsUnion[T]) twoWayMergeInternalStandard(source *ReservoirItemsSketch[T]) error {
	for i := 0; i < source.NumSamples(); i++ {
		v, err := source.valueAtPosition(i)
		if err != nil {
			return err
		}

		if err := u.gadget.Update(v); err != nil {
			return err
		}
	}
	return nil
}

// should be called ONLY by twoWayMergeInternal.
func (u *ReservoirItemsUnion[T]) twoWayMergeInternalWeighted(source *ReservoirItemsSketch[T]) error {
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
			v, err := source.valueAtPosition(i)
			if err != nil {
				return err
			}

			u.gadget.insertValueAtPosition(v, slotNo)
		}
	}

	if err := u.gadget.forceIncrementItemsSeen(source.N()); err != nil {
		return err
	}

	return nil
}

// Result returns a copy of the internal sketch.
func (u *ReservoirItemsUnion[T]) Result() (*ReservoirItemsSketch[T], error) {
	if u.gadget == nil {
		return nil, nil
	}
	return u.gadget.Copy(), nil
}

// MaxK returns the maximum k for this union.
func (u *ReservoirItemsUnion[T]) MaxK() int {
	return u.maxK
}

// Reset clears the union.
func (u *ReservoirItemsUnion[T]) Reset() {
	u.gadget.Reset()
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
	unionSerVer    = 2
	unionFlagEmpty = 0x04
)

// ToSlice serializes the union to a byte slice.
//
// If the sketch contains string values and the caller cares about
// cross-language compatibility, it is the caller's responsibility to ensure
// that the serialized string data is encoded as valid UTF-8.
func (u *ReservoirItemsUnion[T]) ToSlice(serde ItemsSerDe[T]) ([]byte, error) {
	empty := u.gadget == nil || u.gadget.NumSamples() == 0

	if empty {
		buf := make([]byte, 8)
		buf[0] = byte(internal.FamilyEnum.ReservoirUnion.MaxPreLongs)
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

	buf[0] = byte(internal.FamilyEnum.ReservoirUnion.MaxPreLongs)
	buf[1] = unionSerVer
	buf[2] = byte(internal.FamilyEnum.ReservoirUnion.Id)
	buf[3] = 0
	binary.LittleEndian.PutUint32(buf[4:], uint32(u.maxK))

	copy(buf[8:], gadgetBytes)

	return buf, nil
}

// NewReservoirItemsUnionFromSlice deserializes a union from a byte slice.
//
// If the sketch contains string values and the caller cares about
// cross-language compatibility, it is the caller's responsibility to ensure
// that the serialized string data is encoded as valid UTF-8.
func NewReservoirItemsUnionFromSlice[T any](data []byte, serde ItemsSerDe[T]) (*ReservoirItemsUnion[T], error) {
	if len(data) < 8 {
		return nil, errors.New("data too short")
	}

	preambleLongs := int(data[0] & 0x3F)
	if preambleLongs != internal.FamilyEnum.ReservoirUnion.MaxPreLongs {
		return nil, fmt.Errorf("invalid preamble longs: expected %d, got %d", internal.FamilyEnum.ReservoirUnion.MaxPreLongs, preambleLongs)
	}

	ver := data[1]

	family := data[2]
	if family != byte(internal.FamilyEnum.ReservoirUnion.Id) {
		return nil, errors.New("wrong sketch family")
	}

	flags := data[3]
	isEmpty := (flags & unionFlagEmpty) != 0

	maxK := int(binary.LittleEndian.Uint32(data[4:]))

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

	union, err := NewReservoirItemsUnion[T](maxK)
	if err != nil {
		return nil, err
	}

	if isEmpty {
		return union, nil
	}

	if len(data) <= 8 {
		return nil, errors.New("data too short for non-empty union")
	}

	sketchData := data[8:]
	sketch, err := NewReservoirItemsSketchFromSlice[T](sketchData, serde)
	if err != nil {
		return nil, err
	}
	if err := union.UpdateSketch(sketch); err != nil {
		return nil, err
	}

	return union, nil
}
