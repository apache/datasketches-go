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
)

// ArrayOfNumbersSketchEncoder encodes a compact ArrayOfNumberSketch to bytes.
type ArrayOfNumbersSketchEncoder[V Number] struct {
	w     io.Writer
	write func(w io.Writer, summary *ArrayOfNumbersSummary[V]) error
}

// NewArrayOfNumbersSketchEncoder creates a new ArrayOfNumbersSketchEncoder.
func NewArrayOfNumbersSketchEncoder[V Number](w io.Writer) ArrayOfNumbersSketchEncoder[V] {
	return ArrayOfNumbersSketchEncoder[V]{
		w: w,
		write: func(w io.Writer, summary *ArrayOfNumbersSummary[V]) error {
			for _, v := range summary.values {
				if err := binary.Write(w, binary.LittleEndian, v); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// Encode encodes a compact ArrayOfNumbersSketch to bytes.
func (enc *ArrayOfNumbersSketchEncoder[V]) Encode(sketch *ArrayOfNumbersCompactSketch[V]) error {
	preambleLongs := uint8(1)
	if err := binary.Write(enc.w, binary.LittleEndian, preambleLongs); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, ArrayOfNumbersSketchSerialVersion); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, ArrayOfNumbersSketchFamily); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, ArrayOfNumbersSketchType); err != nil {
		return err
	}

	numRetained := sketch.NumRetained()

	// Write flags byte
	var flagsByte uint8
	if sketch.IsEmpty() {
		flagsByte |= 1 << arrayOfNumbersSketchFlagIsEmpty
	}
	if numRetained > 0 {
		flagsByte |= 1 << arrayOfNumbersSketchFlagHasEntries
	}
	if sketch.IsOrdered() {
		flagsByte |= 1 << arrayOfNumbersSketchFlagIsOrdered
	}
	if err := binary.Write(enc.w, binary.LittleEndian, flagsByte); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, sketch.numberOfValuesInSummary); err != nil {
		return err
	}

	seedHash, err := sketch.SeedHash()
	if err != nil {
		return err
	}
	if err := binary.Write(enc.w, binary.LittleEndian, seedHash); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, sketch.theta); err != nil {
		return err
	}

	if numRetained > 0 {
		numEntries := uint32(len(sketch.entries))
		if err := binary.Write(enc.w, binary.LittleEndian, numEntries); err != nil {
			return err
		}

		unused := uint32(0)
		if err := binary.Write(enc.w, binary.LittleEndian, unused); err != nil {
			return err
		}

		for _, entry := range sketch.entries {
			if err := binary.Write(enc.w, binary.LittleEndian, entry.Hash); err != nil {
				return err
			}
		}

		for _, entry := range sketch.entries {
			if err := enc.write(enc.w, entry.Summary); err != nil {
				return err
			}
		}
	}
	return nil
}
