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
	"io"
	"math"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

const (
	varOptItemsUnionSerialVersion = uint8(2)
	varOptItemsUnionEmptyFlag     = uint8(4)
	varOptItemsUnionEmptyPreLongs = uint8(1)
)

// VarOptItemsUnionEncoder writes encoded data to the provided io.Writer
// and uses ItemSketchSerde for custom serialization of items.
type VarOptItemsUnionEncoder[T any] struct {
	w     io.Writer
	serde common.ItemSketchSerde[T]
}

// NewVarOptItemsUnionEncoder creates a new VarOptItemsUnionEncoder
func NewVarOptItemsUnionEncoder[T any](w io.Writer, serde common.ItemSketchSerde[T]) VarOptItemsUnionEncoder[T] {
	return VarOptItemsUnionEncoder[T]{
		w:     w,
		serde: serde,
	}
}

// Encode serializes a VarOptItemsUnion into the underlying writer.
func (enc *VarOptItemsUnionEncoder[T]) Encode(union *VarOptItemsUnion[T]) error {
	if union.gadget.NumSamples() == 0 { // empty case.
		if err := binary.Write(enc.w, binary.LittleEndian, varOptItemsUnionEmptyPreLongs); err != nil {
			return err
		}

		if err := binary.Write(enc.w, binary.LittleEndian, varOptItemsUnionSerialVersion); err != nil {
			return err
		}

		if err := binary.Write(enc.w, binary.LittleEndian, uint8(internal.FamilyEnum.VarOptItemsUnion.Id)); err != nil {
			return err
		}

		if err := binary.Write(enc.w, binary.LittleEndian, varOptItemsUnionEmptyFlag); err != nil {
			return err
		}

		if err := binary.Write(enc.w, binary.LittleEndian, int32(union.k)); err != nil {
			return err
		}

		return nil
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint8(internal.FamilyEnum.VarOptItemsUnion.MaxPreLongs)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, varOptItemsUnionSerialVersion); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint8(internal.FamilyEnum.VarOptItemsUnion.Id)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint8(0)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, int32(union.k)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, union.n); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, math.Float64bits(union.outerTauNumer)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, union.outerTauDenom); err != nil {
		return err
	}

	sketchEncoder := NewVarOptItemsSketchEncoder(enc.w, enc.serde)
	if err := sketchEncoder.Encode(union.gadget); err != nil {
		return err
	}

	return nil
}
