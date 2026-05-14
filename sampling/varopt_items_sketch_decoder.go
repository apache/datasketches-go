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
	"math"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

// TODO: Support Stream I/O.

// Decode reconstructs a VarOptItemsSketch from a byte slice using the provided ItemsSerDe implementation for deserialization.
// Returns the reconstructed VarOptItemsSketch or an error if deserialization fails.
func Decode[T any](buffer []byte, serde common.ItemSketchSerde[T]) (*VarOptItemsSketch[T], error) {
	if len(buffer) < 8 {
		return nil, errors.New("data too short")
	}

	index := 0

	fistBytes := buffer[index]
	index++

	preambleLongs := fistBytes & 0x3F

	rf := (fistBytes >> 6) & 0x03

	serVer := buffer[index]
	index++

	familyID := buffer[index]
	index++

	flags := buffer[index]
	index++

	k := binary.LittleEndian.Uint32(buffer[index:])
	index += 4

	isEmpty := (flags & emptyFlagMask) != 0
	if err := validateVarOptItemsSketchPreambleLongs(preambleLongs, isEmpty); err != nil {
		return nil, err
	}

	if err := validateVarOptItemsSketchFamilyAndSerVer(familyID, serVer); err != nil {
		return nil, err
	}

	isGadget := (flags & gadgetFlagMask) != 0

	if isEmpty {
		return newVarOptItemsSketchFromState[T](int(k), ResizeFactor(rf), isGadget)
	}

	if err := validateBuffer(buffer, index+8); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint64(buffer[index:])
	index += 8

	if err := validateBuffer(buffer, index+4); err != nil {
		return nil, err
	}
	h := binary.LittleEndian.Uint32(buffer[index:])
	index += 4

	if err := validateBuffer(buffer, index+4); err != nil {
		return nil, err
	}
	r := binary.LittleEndian.Uint32(buffer[index:])
	index += 4

	allocSize, err := computeVarOptItemsSketchDataSize(
		preambleLongs, k, n, h, r, ResizeFactor(rf),
	)
	if err != nil {
		return nil, err
	}

	totalWeightR := float64(0)
	// validate R region weight.
	if preambleLongs == preambleLongsFull {
		if err := validateBuffer(buffer, index+8); err != nil {
			return nil, err
		}

		totalWeightR = math.Float64frombits(binary.LittleEndian.Uint64(buffer[index:]))
		index += 8

		if math.IsNaN(totalWeightR) || r == 0 || totalWeightR <= 0 {
			return nil, fmt.Errorf("data is corrupt in full mode: invalid R region weight: %f", totalWeightR)
		}
	}

	sliceLen := int(allocSize)
	if r == 0 {
		sliceLen = int(h)
	}

	// read h weights, fill in rest of slice with -1.0
	weights := make([]float64, sliceLen, allocSize)
	if err := validateBuffer(buffer, index+int(h)*8); err != nil {
		return nil, err
	}
	for i := 0; i < int(h); i++ {
		w := math.Float64frombits(binary.LittleEndian.Uint64(buffer[index:]))
		index += 8

		if w <= 0 {
			return nil, fmt.Errorf("non-positive weight: %f", w)
		}

		weights[i] = w
	}
	for i := h; i < uint32(len(weights)); i++ {
		weights[i] = -1
	}

	var (
		marks       []bool
		numMarksInH uint32
	)
	if isGadget {
		marks = make([]bool, sliceLen, allocSize)
		val := uint8(0)
		for i := 0; i < int(h); i++ {
			if (i & 0x7) == 0 {
				if err := validateBuffer(buffer, index+1); err != nil {
					return nil, err
				}

				val = buffer[index]
				index++
			}

			marks[i] = (val>>(i&0x7))&0x1 == 1
			if marks[i] {
				numMarksInH++
			}
		}
	}

	data := make([]T, sliceLen, allocSize)

	hBytes, err := serde.SizeOfMany(buffer, index, int(h))
	if err != nil {
		return nil, err
	}
	if err := validateBuffer(buffer, index+hBytes); err != nil {
		return nil, err
	}
	hRegionData, err := serde.DeserializeManyFromSlice(buffer, index, int(h))
	if err != nil {
		return nil, err
	}
	index += hBytes
	copy(data[:h], hRegionData)

	if r > 0 {
		rBytes, err := serde.SizeOfMany(buffer, index, int(r))
		if err != nil {
			return nil, err
		}
		if err := validateBuffer(buffer, index+rBytes); err != nil {
			return nil, err
		}
		rData, err := serde.DeserializeManyFromSlice(buffer, index, int(r))
		if err != nil {
			return nil, err
		}
		index += rBytes

		copy(data[h+1:h+1+r], rData)
	}

	m := 0
	if r > 0 {
		m = 1
	}
	return &VarOptItemsSketch[T]{
		data:         data,
		weights:      weights,
		k:            int(k),
		h:            int(h),
		m:            m,
		r:            int(r),
		n:            int64(n),
		totalWeightR: totalWeightR,
		rf:           ResizeFactor(rf),
		marks:        marks,
		numMarksInH:  numMarksInH,
	}, nil
}

func validateVarOptItemsSketchPreambleLongs(preambleLongs uint8, isEmpty bool) error {
	if isEmpty {
		if preambleLongs != preambleLongsEmpty {
			return fmt.Errorf("invalid preamble longs: expected empty, got %d", preambleLongs)
		}
	} else if preambleLongs != preambleLongsWarmup && preambleLongs != preambleLongsFull {
		return fmt.Errorf("invalid preamble longs: expected warmup or full, got %d", preambleLongs)
	}
	return nil
}

func validateVarOptItemsSketchFamilyAndSerVer(familyID, serVer uint8) error {
	if int(familyID) == internal.FamilyEnum.VarOptItems.Id {
		if serVer != varOptItemsSketchSerialVersion {
			return fmt.Errorf("invalid serialization version: expected %d, got %d", varOptItemsSketchSerialVersion, serVer)
		}
		return nil
	}

	return fmt.Errorf("invalid family ID: expected %d, got %d", internal.FamilyEnum.VarOptItems.Id, familyID)
}

func computeVarOptItemsSketchDataSize(
	preambleLongs uint8, k uint32, n uint64, h uint32, r uint32, rf ResizeFactor,
) (uint32, error) {
	if k == 0 || k > varOptMaxK {
		return 0, errors.New("k must be at least 1 and less than 2^31 - 1")
	}

	allocSize := 0
	if n <= uint64(k) {
		if preambleLongs != preambleLongsWarmup {
			return 0, fmt.Errorf("invalid preamble longs: expected warmup because n<=k, got %d", preambleLongs)
		}

		if n != uint64(h) {
			return 0, fmt.Errorf("invalid state in warmup mode: expected n==h, got n=%d, h=%d", n, h)
		}

		if r > 0 {
			return 0, fmt.Errorf("invalid state in warmup mode: expected r==0, got r=%d", r)
		}

		ceilingLgK := math.Log2(float64(common.CeilingPowerOf2(int(k))))
		minLgSize := math.Log2(float64(common.CeilingPowerOf2(int(h))))
		initialLgSize := startingSubMultiple(int(ceilingLgK), int(rf), int(minLgSize))
		allocSize = adjustedSamplingAllocationSize(int(k), 1<<initialLgSize)
		if allocSize == int(k) {
			allocSize++
		}
	} else {
		if preambleLongs != preambleLongsFull {
			return 0, fmt.Errorf("invalid preamble longs: expected full because n>k, got %d", preambleLongs)
		}

		if h+r != k {
			return 0, fmt.Errorf("invalid state in full mode: expected h+r==k, got h=%d, r=%d, k=%d", h, r, k)
		}

		allocSize = int(k) + 1
	}

	return uint32(allocSize), nil
}

func validateBuffer(buf []byte, endIndex int) error {
	if len(buf) < endIndex {
		return io.ErrUnexpectedEOF
	}
	return nil
}
