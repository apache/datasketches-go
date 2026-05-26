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
	"fmt"
	"io"
	"math"

	"github.com/apache/datasketches-go/internal"
)

// Decoder is responsible for decoding sketches from binary format.
type Decoder struct{}

// NewDecoder creates a new instance of Decoder.
func NewDecoder() Decoder {
	return Decoder{}
}

// Decode decodes a sketch from the provided reader.
func (d *Decoder) Decode(r io.Reader) (*Sketch, error) {
	var preambleInt byte
	if err := binary.Read(r, binary.LittleEndian, &preambleInt); err != nil {
		return nil, err
	}

	var serVer byte
	if err := binary.Read(r, binary.LittleEndian, &serVer); err != nil {
		return nil, err
	}
	if serVer != serialVersion {
		return nil, fmt.Errorf("unsupported serialization version: %d", serVer)
	}

	var familyID byte
	if err := binary.Read(r, binary.LittleEndian, &familyID); err != nil {
		return nil, err
	}
	if int(familyID) != internal.FamilyEnum.REQ.Id {
		return nil, fmt.Errorf("invalid family id: %d", familyID)
	}

	var flags byte
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return nil, err
	}
	isEmpty := (flags & 4) > 0
	isHighRankAccuracyMode := (flags & 8) > 0
	isRawItemsSketch := (flags & 16) > 0
	isLevel0Sorted := (flags & 32) > 0

	var k uint16
	if err := binary.Read(r, binary.LittleEndian, &k); err != nil {
		return nil, err
	}

	var numCompactors byte
	if err := binary.Read(r, binary.LittleEndian, &numCompactors); err != nil {
		return nil, err
	}

	var numRawItems byte
	if err := binary.Read(r, binary.LittleEndian, &numRawItems); err != nil {
		return nil, err
	}

	format := inferEncodingFormat(isEmpty, isRawItemsSketch, int(numCompactors))
	switch format {
	case encodingFormatEmpty:
		if preambleInt != 2 {
			return nil, fmt.Errorf("invalid preamble: %d", preambleInt)
		}
		return NewSketch(WithK(int(k)), WithHighRankAccuracyMode(isHighRankAccuracyMode))
	case encodingFormatRawItems:
		if preambleInt != 2 {
			return nil, fmt.Errorf("invalid preamble: %d", preambleInt)
		}

		sk, err := NewSketch(WithK(int(k)), WithHighRankAccuracyMode(isHighRankAccuracyMode))
		if err != nil {
			return nil, err
		}

		for i := byte(0); i < numRawItems; i++ {
			var rawItem uint32
			if err := binary.Read(r, binary.LittleEndian, &rawItem); err != nil {
				return nil, err
			}

			if err := sk.Update(math.Float32frombits(rawItem)); err != nil {
				return nil, err
			}
		}
		return sk, nil
	case encodingFormatExact:
		if preambleInt != 2 {
			return nil, fmt.Errorf("invalid preamble: %d", preambleInt)
		}

		decoder := newCompactorDecoder(isLevel0Sorted, isHighRankAccuracyMode)
		result, err := decoder.Decode(r)
		if err != nil {
			return nil, err
		}

		sk := &Sketch{
			n:                      result.n,
			compactors:             []*compactor{result.compactor},
			minItem:                result.minItem,
			maxItem:                result.maxItem,
			k:                      int(k),
			isHighRankAccuracyMode: isHighRankAccuracyMode,
		}
		if err := sk.validateK(); err != nil {
			return nil, err
		}
		sk.maxNomSize = sk.computeMaxNomSize()
		sk.numRetained = sk.computeRetainedItems()
		return sk, nil
	default: // Estimation.
		if preambleInt != 4 {
			return nil, fmt.Errorf("invalid preamble: %d", preambleInt)
		}

		var n uint64
		if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
			return nil, err
		}

		var minItemRaw uint32
		if err := binary.Read(r, binary.LittleEndian, &minItemRaw); err != nil {
			return nil, err
		}
		minItem := math.Float32frombits(minItemRaw)

		var maxItemRaw uint32
		if err := binary.Read(r, binary.LittleEndian, &maxItemRaw); err != nil {
			return nil, err
		}
		maxItem := math.Float32frombits(maxItemRaw)

		compactors := make([]*compactor, 0, int(numCompactors))
		for i := 0; i < int(numCompactors); i++ {
			if i == 0 {
				decoder := newCompactorDecoder(isLevel0Sorted, isHighRankAccuracyMode)
				result, err := decoder.Decode(r)
				if err != nil {
					return nil, err
				}

				compactors = append(compactors, result.compactor)
			} else {
				decoder := newCompactorDecoder(true, isHighRankAccuracyMode)
				result, err := decoder.Decode(r)
				if err != nil {
					return nil, err
				}

				compactors = append(compactors, result.compactor)
			}
		}

		sk := &Sketch{
			k:                      int(k),
			isHighRankAccuracyMode: isHighRankAccuracyMode,
			n:                      int64(n),
			minItem:                minItem,
			maxItem:                maxItem,
			compactors:             compactors,
		}
		if err := sk.validateK(); err != nil {
			return nil, err
		}
		sk.maxNomSize = sk.computeMaxNomSize()
		sk.numRetained = sk.computeRetainedItems()
		return sk, nil
	}
}

// Decode decodes a sketch from the provided buffer.
// If the buffer is too short, returns io.ErrUnexpectedEOF.
func Decode(buf []byte) (*Sketch, error) {
	index := 0
	if err := validateBuffer(buf, index+1); err != nil {
		return nil, err
	}
	preambleInts := buf[index]
	index++

	if err := validateBuffer(buf, index+1); err != nil {
		return nil, err
	}
	serVer := buf[index]
	index++
	if serVer != serialVersion {
		return nil, fmt.Errorf("unsupported serialization version: %d", serVer)
	}

	if err := validateBuffer(buf, index+1); err != nil {
		return nil, err
	}
	familyID := buf[index]
	index++
	if int(familyID) != internal.FamilyEnum.REQ.Id {
		return nil, fmt.Errorf("invalid family id: %d", familyID)
	}

	// flags.
	if err := validateBuffer(buf, index+1); err != nil {
		return nil, err
	}
	flags := buf[index]
	index++
	isEmpty := (flags & 4) > 0
	isHighRankAccuracyMode := (flags & 8) > 0
	isRawItemsSketch := (flags & 16) > 0
	isLevel0Sorted := (flags & 32) > 0

	if err := validateBuffer(buf, index+2); err != nil {
		return nil, err
	}
	k := binary.LittleEndian.Uint16(buf[index : index+2])
	index += 2

	if err := validateBuffer(buf, index+1); err != nil {
		return nil, err
	}
	numCompactors := buf[index]
	index++

	if err := validateBuffer(buf, index+1); err != nil {
		return nil, err
	}
	numRawItems := buf[index]
	index++

	format := inferEncodingFormat(isEmpty, isRawItemsSketch, int(numCompactors))
	switch format {
	case encodingFormatEmpty:
		if preambleInts != 2 {
			return nil, fmt.Errorf("invalid preamble ints for empty encoding format: %d", preambleInts)
		}
		return NewSketch(WithK(int(k)), WithHighRankAccuracyMode(isHighRankAccuracyMode))
	case encodingFormatRawItems:
		if preambleInts != 2 {
			return nil, fmt.Errorf("invalid preamble ints for raw items encoding: %d", preambleInts)
		}

		sk, err := NewSketch(WithK(int(k)), WithHighRankAccuracyMode(isHighRankAccuracyMode))
		if err != nil {
			return nil, err
		}

		for i := 0; i < int(numRawItems); i++ {
			if err := validateBuffer(buf, index+4); err != nil {
				return nil, err
			}

			item := math.Float32frombits(binary.LittleEndian.Uint32(buf[index : index+4]))

			if err := sk.Update(item); err != nil {
				return nil, err
			}

			index += 4
		}
		return sk, nil
	case encodingFormatExact:
		if preambleInts != 2 {
			return nil, fmt.Errorf("invalid preamble ints for exact encoding: %d", preambleInts)
		}

		result, err := decodeCompactor(buf, index, isLevel0Sorted, isHighRankAccuracyMode)
		if err != nil {
			return nil, err
		}

		sk := &Sketch{
			n:                      result.n,
			compactors:             []*compactor{result.compactor},
			minItem:                result.minItem,
			maxItem:                result.maxItem,
			k:                      int(k),
			isHighRankAccuracyMode: isHighRankAccuracyMode,
		}
		if err := sk.validateK(); err != nil {
			return nil, err
		}
		sk.maxNomSize = sk.computeMaxNomSize()
		sk.numRetained = sk.computeRetainedItems()
		return sk, nil
	default: // Estimation.
		if err := validateBuffer(buf, index+8); err != nil {
			return nil, err
		}
		n := binary.LittleEndian.Uint64(buf[index : index+8])
		index += 8

		if err := validateBuffer(buf, index+4); err != nil {
			return nil, err
		}
		minItem := math.Float32frombits(binary.LittleEndian.Uint32(buf[index : index+4]))
		index += 4

		if err := validateBuffer(buf, index+4); err != nil {
			return nil, err
		}
		maxItem := math.Float32frombits(binary.LittleEndian.Uint32(buf[index : index+4]))
		index += 4

		compactors := make([]*compactor, 0, int(numCompactors))
		for i := 0; i < int(numCompactors); i++ {
			if i == 0 {
				res, err := decodeCompactor(buf, index, isLevel0Sorted, isHighRankAccuracyMode)
				if err != nil {
					return nil, err
				}

				compactors = append(compactors, res.compactor)
				index = res.bufferEndIndex
			} else {
				res, err := decodeCompactor(buf, index, true, isHighRankAccuracyMode)
				if err != nil {
					return nil, err
				}

				compactors = append(compactors, res.compactor)
				index = res.bufferEndIndex
			}
		}

		sk := &Sketch{
			k:                      int(k),
			isHighRankAccuracyMode: isHighRankAccuracyMode,
			n:                      int64(n),
			minItem:                minItem,
			maxItem:                maxItem,
			compactors:             compactors,
		}
		if err := sk.validateK(); err != nil {
			return nil, err
		}
		sk.maxNomSize = sk.computeMaxNomSize()
		sk.numRetained = sk.computeRetainedItems()
		return sk, nil
	}
}

func validateBuffer(buf []byte, endIndex int) error {
	if len(buf) < endIndex {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func inferEncodingFormat(isEmpty, isRawItemsSketch bool, numCompactors int) encodingFormat {
	if numCompactors <= 1 {
		if isEmpty {
			return encodingFormatEmpty
		}
		if isRawItemsSketch {
			return encodingFormatRawItems
		}
		return encodingFormatExact
	}
	return encodingFormatEstimation
}
