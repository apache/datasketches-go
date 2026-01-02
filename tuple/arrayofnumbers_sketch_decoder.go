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
	"bytes"
	"encoding/binary"
	"io"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/theta"
)

// ArrayOfNumbersSketchDecoder decodes a compact ArrayOfNumbersSketch from the given reader.
type ArrayOfNumbersSketchDecoder[V Number] struct {
	seed uint64
	read func(r io.Reader, numberOfValuesInSummary uint8) (*ArrayOfNumbersSummary[V], error)
}

// NewArrayOfNumbersSketchDecoderDecoder creates a new decoder.
func NewArrayOfNumbersSketchDecoderDecoder[V Number](seed uint64) ArrayOfNumbersSketchDecoder[V] {
	return ArrayOfNumbersSketchDecoder[V]{
		seed: seed,
		read: func(r io.Reader, numberOfValuesInSummary uint8) (*ArrayOfNumbersSummary[V], error) {
			values := make([]V, 0, numberOfValuesInSummary)
			for i := 0; i < int(numberOfValuesInSummary); i++ {
				var value V
				if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
					return nil, err
				}

				values = append(values, value)
			}

			return newArrayOfNumbersSummaryFromValues[V](values, numberOfValuesInSummary), nil
		},
	}
}

// Decode decodes a compact sketch from the given reader.
func (dec *ArrayOfNumbersSketchDecoder[V]) Decode(r io.Reader) (*ArrayOfNumbersCompactSketch[V], error) {
	var preambleLongs uint8
	if err := binary.Read(r, binary.LittleEndian, &preambleLongs); err != nil {
		return nil, err
	}

	var serialVersion uint8
	if err := binary.Read(r, binary.LittleEndian, &serialVersion); err != nil {
		return nil, err
	}

	var family uint8
	if err := binary.Read(r, binary.LittleEndian, &family); err != nil {
		return nil, err
	}

	var sketchType uint8
	if err := binary.Read(r, binary.LittleEndian, &sketchType); err != nil {
		return nil, err
	}

	var flags uint8
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, err
	}

	var numberOfValuesInSummary uint8
	if err := binary.Read(r, binary.LittleEndian, &numberOfValuesInSummary); err != nil {
		return nil, err
	}

	var seedHash uint16
	if err := binary.Read(r, binary.LittleEndian, &seedHash); err != nil {
		return nil, err
	}

	if err := theta.CheckSerialVersionEqual(serialVersion, ArrayOfNumbersSketchSerialVersion); err != nil {
		return nil, err
	}

	if err := theta.CheckSketchFamilyEqual(family, ArrayOfNumbersSketchFamily); err != nil {
		return nil, err
	}

	if err := theta.CheckSketchTypeEqual(sketchType, ArrayOfNumbersSketchType); err != nil {
		return nil, err
	}

	hasEntries := (flags & (1 << arrayOfNumbersSketchFlagHasEntries)) != 0
	if hasEntries {
		expectedSeedHash, err := internal.ComputeSeedHash(int64(dec.seed))
		if err != nil {
			return nil, err
		}
		if err := theta.CheckSeedHashEqual(seedHash, uint16(expectedSeedHash)); err != nil {
			return nil, err
		}
	}

	var thetaVal uint64
	if err := binary.Read(r, binary.LittleEndian, &thetaVal); err != nil {
		return nil, err
	}

	var entries []entry[*ArrayOfNumbersSummary[V]]
	if hasEntries {
		var numEntries uint32
		if err := binary.Read(r, binary.LittleEndian, &numEntries); err != nil {
			return nil, err
		}

		var unused uint32
		if err := binary.Read(r, binary.LittleEndian, &unused); err != nil {
			return nil, err
		}

		hashes := make([]uint64, 0, numEntries)
		for i := uint32(0); i < numEntries; i++ {
			var hash uint64
			if err := binary.Read(r, binary.LittleEndian, &hash); err != nil {
				return nil, err
			}

			hashes = append(hashes, hash)
		}

		entries = make([]entry[*ArrayOfNumbersSummary[V]], 0, numEntries)
		for i := uint32(0); i < numEntries; i++ {
			summary, err := dec.read(r, numberOfValuesInSummary)
			if err != nil {
				return nil, err
			}

			entries = append(entries, entry[*ArrayOfNumbersSummary[V]]{
				Hash:    hashes[i],
				Summary: summary,
			})
		}
	}

	isEmpty := (flags & (1 << arrayOfNumbersSketchFlagIsEmpty)) != 0
	isOrdered := (flags & (1 << arrayOfNumbersSketchFlagIsOrdered)) != 0

	return newArrayOfNumbersCompactSketch[V](
		isEmpty, isOrdered, seedHash, thetaVal, entries, numberOfValuesInSummary,
	), nil
}

// DecodeArrayOfNumbersCompactSketch reconstructs an ArrayOfNumbersCompactSketch from a byte slice using a specified seed.
func DecodeArrayOfNumbersCompactSketch[V Number](b []byte, seed uint64) (*ArrayOfNumbersCompactSketch[V], error) {
	decoder := NewArrayOfNumbersSketchDecoderDecoder[V](seed)
	return decoder.Decode(bytes.NewReader(b))
}
