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
	"fmt"
	"io"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/theta"
)

// SummaryReader reads and returns a summary from the reader.
// Implementations should read the format written by a corresponding SummaryWriter.
type SummaryReader[S Summary] func(r io.Reader) (S, error)

// Decoder decodes a compact sketch from the given reader.
type Decoder[S Summary] struct {
	seed uint64
	read SummaryReader[S]
}

// NewDecoder creates a new decoder.
func NewDecoder[S Summary](seed uint64, read SummaryReader[S]) Decoder[S] {
	return Decoder[S]{
		seed: seed,
		read: read,
	}
}

// Decode decodes a compact sketch from the given reader.
func (dec *Decoder[S]) Decode(r io.Reader) (*CompactSketch[S], error) {
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

	var unused uint8
	if err := binary.Read(r, binary.LittleEndian, &unused); err != nil {
		return nil, err
	}

	var flags uint8
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, err
	}

	var seedHash uint16
	if err := binary.Read(r, binary.LittleEndian, &seedHash); err != nil {
		return nil, err
	}

	if serialVersion != SerialVersion && serialVersion != SerialVersionLegacy {
		return nil, fmt.Errorf("serial version mismatch: expected %d, actual %d", SerialVersion, serialVersion)
	}

	if err := theta.CheckSketchFamilyEqual(family, SketchFamily); err != nil {
		return nil, err
	}

	if sketchType != SketchType && sketchType != SketchTypeLegacy {
		return nil, fmt.Errorf("sketch type mismatch: expected %d, actual %d", SketchType, sketchType)
	}

	isEmpty := (flags & (1 << flagIsEmpty)) != 0
	if !isEmpty {
		expectedSeedHash, err := internal.ComputeSeedHash(int64(dec.seed))
		if err != nil {
			return nil, err
		}
		if err := theta.CheckSeedHashEqual(seedHash, uint16(expectedSeedHash)); err != nil {
			return nil, err
		}
	}

	theta := theta.MaxTheta
	numEntries := uint32(0)
	if !isEmpty {
		if preambleLongs == 1 {
			numEntries = 1
		} else {
			if err := binary.Read(r, binary.LittleEndian, &numEntries); err != nil {
				return nil, err
			}

			unused := uint32(0)
			if err := binary.Read(r, binary.LittleEndian, &unused); err != nil {
				return nil, err
			}

			if preambleLongs > 2 {
				if err := binary.Read(r, binary.LittleEndian, &theta); err != nil {
					return nil, err
				}
			}
		}
	}

	entries := make([]entry[S], numEntries)
	for i := uint32(0); i < numEntries; i++ {
		var hash uint64
		if err := binary.Read(r, binary.LittleEndian, &hash); err != nil {
			return nil, err
		}

		summary, err := dec.read(r)
		if err != nil {
			return nil, err
		}

		entries[i] = entry[S]{Hash: hash, Summary: summary}
	}

	isOrdered := (flags & (1 << flagIsOrdered)) != 0
	return newCompactSketch[S](
		isEmpty, isOrdered, seedHash, theta, entries,
	), nil
}

// Decode reconstructs a CompactSketch from a byte slice using a specified seed and read function.
func Decode[S Summary](b []byte, seed uint64, read SummaryReader[S]) (*CompactSketch[S], error) {
	decoder := NewDecoder[S](seed, read)
	return decoder.Decode(bytes.NewReader(b))
}
