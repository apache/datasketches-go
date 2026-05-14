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
	"io"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

const (
	preambleLongsEmpty             = uint8(1)
	preambleLongsWarmup            = uint8(3)
	preambleLongsFull              = uint8(4)
	varOptItemsSketchSerialVersion = uint8(2)
	gadgetFlagMask                 = uint8(128)
	emptyFlagMask                  = uint8(4)
)

// VarOptItemsSketchEncoder writes encoded data to the provided io.Writer and uses ItemsSerDe for custom serialization of items.
type VarOptItemsSketchEncoder[T any] struct {
	w     io.Writer
	serde common.ItemSketchSerde[T]
}

// NewVarOptItemsSketchEncoder creates a new VarOptItemsSketchEncoder.
func NewVarOptItemsSketchEncoder[T any](
	w io.Writer,
	serde common.ItemSketchSerde[T],
) VarOptItemsSketchEncoder[T] {
	return VarOptItemsSketchEncoder[T]{
		w:     w,
		serde: serde,
	}
}

// Encode writes the provided VarOptItemsSketch to the underlying io.Writer.
func (enc *VarOptItemsSketchEncoder[T]) Encode(sketch *VarOptItemsSketch[T]) error {
	if sketch == nil {
		return errors.New("cannot encode nil VarOptItemsSketch")
	}

	isEmpty := sketch.n == 0 && sketch.r == 0

	preambleLongs := preambleLongsFull
	if isEmpty {
		preambleLongs = preambleLongsEmpty
	} else if sketch.r == 0 {
		preambleLongs = preambleLongsWarmup
	}

	firstByte := (preambleLongs & 0x3F) | (uint8(sketch.rf) << 6)
	if err := binary.Write(enc.w, binary.LittleEndian, firstByte); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, varOptItemsSketchSerialVersion); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint8(internal.FamilyEnum.VarOptItems.Id)); err != nil {
		return err
	}

	flags := uint8(0)
	if sketch.marks != nil {
		flags |= gadgetFlagMask
	}
	if isEmpty {
		flags |= emptyFlagMask
	}
	if err := binary.Write(enc.w, binary.LittleEndian, flags); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint32(sketch.k)); err != nil {
		return err
	}

	if isEmpty {
		return nil
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint64(sketch.n)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint32(sketch.h)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint32(sketch.r)); err != nil {
		return err
	}

	if sketch.r > 0 {
		if err := binary.Write(enc.w, binary.LittleEndian, sketch.totalWeightR); err != nil {
			return err
		}
	}

	for i := 0; i < sketch.h; i++ {
		if i >= len(sketch.weights) {
			return fmt.Errorf("invalid weights array size: %d, h: %d", len(sketch.weights), sketch.h)
		}

		if err := binary.Write(enc.w, binary.LittleEndian, sketch.weights[i]); err != nil {
			return err
		}
	}

	if sketch.marks != nil {
		val := uint8(0)
		for i := 0; i < sketch.h; i++ {
			if sketch.marks[i] {
				val |= 0x1 << (i & 0x7)
			}

			if i&0x7 == 0x7 {
				if err := binary.Write(enc.w, binary.LittleEndian, val); err != nil {
					return err
				}
				val = 0
			}
		}

		// write out any remaining values.
		if sketch.h&0x7 > 0 {
			if err := binary.Write(enc.w, binary.LittleEndian, val); err != nil {
				return err
			}
		}
	}

	expectedDataLen := sketch.h
	if sketch.r > 0 {
		expectedDataLen += sketch.r + 1
	}
	if len(sketch.data) != expectedDataLen {
		return fmt.Errorf("invalid data array size: %d, h: %d, r: %d", len(sketch.data), sketch.h, sketch.r)
	}

	b := enc.serde.SerializeManyToSlice(sketch.data[:sketch.h])
	if _, err := enc.w.Write(b); err != nil {
		return err
	}

	if sketch.r > 0 {
		rStart := sketch.h + 1 // skip gap
		rEnd := rStart + sketch.r
		b = enc.serde.SerializeManyToSlice(sketch.data[rStart:rEnd])
		if _, err := enc.w.Write(b); err != nil {
			return err
		}
	}

	return nil
}
