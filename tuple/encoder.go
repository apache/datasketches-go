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
	"encoding/binary"
	"io"

	"github.com/apache/datasketches-go/internal"
)

// SummaryWriter writes a summary to the writer.
// Implementations should write the summary in a format that can be read by a corresponding SummaryReader.
type SummaryWriter[S Summary] func(w io.Writer, s S) error

// Encoder encodes a compact tuple sketch to bytes.
type Encoder[S Summary] struct {
	w     io.Writer
	write SummaryWriter[S]
}

// NewEncoder creates a new encoder.
func NewEncoder[S Summary](w io.Writer, write SummaryWriter[S]) Encoder[S] {
	return Encoder[S]{w: w, write: write}
}

// Encode encodes a compact tuple sketch to bytes.
func (enc *Encoder[S]) Encode(sketch *CompactSketch[S]) error {
	preambleLongs := sketch.preambleLongs()
	if err := binary.Write(enc.w, binary.LittleEndian, preambleLongs); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, SerialVersion); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint8(internal.FamilyEnum.Tuple.Id)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, SketchTypeCompactTuple); err != nil {
		return err
	}

	unused := uint8(0)
	if err := binary.Write(enc.w, binary.LittleEndian, unused); err != nil {
		return err
	}

	// Write flags byte
	var flagsByte uint8
	flagsByte |= 1 << flagIsCompact
	flagsByte |= 1 << flagIsReadOnly
	if sketch.IsEmpty() {
		flagsByte |= 1 << flagIsEmpty
	}
	if sketch.IsOrdered() {
		flagsByte |= 1 << flagIsOrdered
	}
	if err := binary.Write(enc.w, binary.LittleEndian, flagsByte); err != nil {
		return err
	}

	seedHash, err := sketch.SeedHash()
	if err != nil {
		return err
	}
	if err := binary.Write(enc.w, binary.LittleEndian, seedHash); err != nil {
		return err
	}

	if preambleLongs > 1 {
		numEntries := uint32(len(sketch.entries))
		if err := binary.Write(enc.w, binary.LittleEndian, numEntries); err != nil {
			return err
		}

		unused := uint32(0)
		if err := binary.Write(enc.w, binary.LittleEndian, unused); err != nil {
			return err
		}
	}
	if sketch.IsEstimationMode() {
		if err := binary.Write(enc.w, binary.LittleEndian, sketch.theta); err != nil {
			return err
		}
	}
	for _, entry := range sketch.entries {
		if err := binary.Write(enc.w, binary.LittleEndian, entry.Hash); err != nil {
			return err
		}

		if err := enc.write(enc.w, entry.Summary); err != nil {
			return err
		}
	}
	return nil
}
