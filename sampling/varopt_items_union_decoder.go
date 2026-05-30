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
	"fmt"
	"math"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

// TODO: Support Stream I/O

// DecodeVarOptItemsUnion deserializes a VarOptItemsUnion from the given byte buffer
// using the provided serde implementation.
func DecodeVarOptItemsUnion[T any](buffer []byte, serde common.ItemSketchSerde[T]) (*VarOptItemsUnion[T], error) {
	index := 0

	if err := validateBuffer(buffer, index+1); err != nil {
		return nil, err
	}
	preLongs := buffer[index]
	index++
	if preLongs != varOptItemsUnionEmptyPreLongs && int(preLongs) != internal.FamilyEnum.VarOptItemsUnion.MaxPreLongs {
		return nil, fmt.Errorf("invalid preLongs: %d", preLongs)
	}

	if err := validateBuffer(buffer, index+1); err != nil {
		return nil, err
	}
	serVer := buffer[index]
	index++
	if serVer != varOptItemsUnionSerialVersion {
		return nil, fmt.Errorf("invalid serial version: %d", serVer)
	}

	if err := validateBuffer(buffer, index+1); err != nil {
		return nil, err
	}
	familyID := buffer[index]
	index++
	if int(familyID) != internal.FamilyEnum.VarOptItemsUnion.Id {
		return nil, fmt.Errorf("invalid family ID: %d", familyID)
	}

	if err := validateBuffer(buffer, index+1); err != nil {
		return nil, err
	}
	isEmpty := (buffer[index] & varOptItemsUnionEmptyFlag) != 0
	index++

	if err := validateBuffer(buffer, index+4); err != nil {
		return nil, err
	}
	maxK := int(binary.LittleEndian.Uint32(buffer[index:]))
	index += 4

	if isEmpty {
		union, err := NewVarOptItemsUnion[T](uint(maxK))
		if err != nil {
			return nil, err
		}

		gadget, err := newVarOptItemsSketchFromState[T](maxK, defaultResizeFactor, true)
		if err != nil {
			return nil, err
		}

		union.gadget = gadget

		return union, nil
	}

	if err := validateBuffer(buffer, index+8); err != nil {
		return nil, err
	}
	n := int64(binary.LittleEndian.Uint64(buffer[index:]))
	index += 8

	if err := validateBuffer(buffer, index+8); err != nil {
		return nil, err
	}
	outerTauNumer := math.Float64frombits(binary.LittleEndian.Uint64(buffer[index:]))
	index += 8

	if err := validateBuffer(buffer, index+8); err != nil {
		return nil, err
	}
	outerTauDenom := binary.LittleEndian.Uint64(buffer[index:])
	index += 8

	gadget, err := DecodeVarOptItemsSketch[T](buffer[index:], serde)
	if err != nil {
		return nil, err
	}

	return &VarOptItemsUnion[T]{
		gadget:        gadget,
		k:             maxK,
		n:             n,
		outerTauNumer: outerTauNumer,
		outerTauDenom: int64(outerTauDenom),
	}, nil
}
