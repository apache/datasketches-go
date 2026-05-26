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

package req

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/apache/datasketches-go/internal"
)

type encodingFormat int

const (
	encodingFormatEmpty      encodingFormat = iota
	encodingFormatRawItems   encodingFormat = iota
	encodingFormatExact      encodingFormat = iota
	encodingFormatEstimation encodingFormat = iota
)

const (
	serialVersion = 1
)

// Encoder encodes a REQ sketch to bytes.
type Encoder struct {
	w io.Writer
}

// NewEncoder creates a new encoder.
func NewEncoder(w io.Writer) Encoder {
	return Encoder{w: w}
}

// Encode encodes a REQ sketch to bytes.
func (enc *Encoder) Encode(sketch *Sketch) error {
	format := sketch.computeEncodingFormat()

	preambleInts := byte(2)
	if format == encodingFormatEstimation {
		preambleInts = byte(4)
	}
	if err := binary.Write(enc.w, binary.LittleEndian, preambleInts); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, byte(serialVersion)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, byte(internal.FamilyEnum.REQ.Id)); err != nil {
		return err
	}

	flags := sketch.encodingFlags()
	if err := binary.Write(enc.w, binary.LittleEndian, byte(flags)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint16(sketch.K())); err != nil {
		return err
	}

	numCompactors := byte(0)
	if !sketch.IsEmpty() {
		numCompactors = byte(sketch.numLevels())
	}
	if err := binary.Write(enc.w, binary.LittleEndian, numCompactors); err != nil {
		return err
	}

	numRawItems := byte(0)
	if sketch.N() <= minK {
		numRawItems = byte(sketch.N())
	}
	if err := binary.Write(enc.w, binary.LittleEndian, numRawItems); err != nil {
		return err
	}

	switch format {
	case encodingFormatEmpty:
		return nil
	case encodingFormatRawItems:
		c0 := sketch.compactors[0]
		for i := 0; i < int(numRawItems); i++ {
			if err := binary.Write(enc.w, binary.LittleEndian, math.Float32bits(c0.Item(i))); err != nil {
				return err
			}
		}
		return nil
	case encodingFormatExact:
		c0 := sketch.compactors[0]
		b, err := c0.MarshalBinary()
		if err != nil {
			return err
		}

		if _, err := enc.w.Write(b); err != nil {
			return err
		}
		return nil
	default: // Estimation.
		if err := binary.Write(enc.w, binary.LittleEndian, uint64(sketch.N())); err != nil {
			return err
		}

		if err := binary.Write(enc.w, binary.LittleEndian, math.Float32bits(sketch.minItem)); err != nil {
			return err
		}

		if err := binary.Write(enc.w, binary.LittleEndian, math.Float32bits(sketch.maxItem)); err != nil {
			return err
		}

		for i := 0; i < int(numCompactors); i++ {
			c := sketch.compactors[i]
			b, err := c.MarshalBinary()
			if err != nil {
				return err
			}

			if _, err := enc.w.Write(b); err != nil {
				return err
			}
		}
		return nil
	}
}
