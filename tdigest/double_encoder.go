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

package tdigest

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/apache/datasketches-go/internal"
)

// DoubleEncoder encodes a Double to bytes.
type DoubleEncoder struct {
	w          io.Writer
	withBuffer bool
}

// NewDoubleEncoder creates a new encoder.
func NewDoubleEncoder(w io.Writer, withBuffer bool) DoubleEncoder {
	return DoubleEncoder{
		w:          w,
		withBuffer: withBuffer,
	}
}

// Encode encodes a Double to bytes.
func (enc *DoubleEncoder) Encode(sketch *Double) error {
	if !enc.withBuffer {
		sketch.compress() // side effect
	}

	if err := binary.Write(enc.w, binary.LittleEndian, sketch.preambleLongs()); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, serialVersion); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint8(internal.FamilyEnum.TDigest.Id)); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, sketch.k); err != nil {
		return err
	}

	var flagsByte byte
	if sketch.IsEmpty() {
		flagsByte |= 1 << serializationFlagIsEmpty
	}
	if sketch.isSingleValue() {
		flagsByte |= 1 << serializationFlagIsSingleValue
	}
	if sketch.reverseMerge {
		flagsByte |= 1 << serializationFlagReverseMerge
	}
	if err := binary.Write(enc.w, binary.LittleEndian, flagsByte); err != nil {
		return err
	}

	var unused uint16
	if err := binary.Write(enc.w, binary.LittleEndian, unused); err != nil {
		return err
	}

	if sketch.IsEmpty() {
		return nil
	}

	if sketch.isSingleValue() {
		if err := binary.Write(enc.w, binary.LittleEndian, sketch.min); err != nil {
			return err
		}

		return nil
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint32(len(sketch.centroids))); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, uint32(len(sketch.buffer))); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, sketch.min); err != nil {
		return err
	}

	if err := binary.Write(enc.w, binary.LittleEndian, sketch.max); err != nil {
		return err
	}

	for _, c := range sketch.centroids {
		if err := binary.Write(enc.w, binary.LittleEndian, c.mean); err != nil {
			return err
		}

		if err := binary.Write(enc.w, binary.LittleEndian, c.weight); err != nil {
			return err
		}
	}

	if len(sketch.buffer) > 0 {
		for _, v := range sketch.buffer {
			if err := binary.Write(enc.w, binary.LittleEndian, v); err != nil {
				return err
			}
		}
	}

	return nil
}

// EncodeDouble encodes a Double to bytes.
func EncodeDouble(sketch *Double, withBuffer bool) ([]byte, error) {
	if !withBuffer {
		sketch.compress() // side effect
	}

	offset := 0
	buf := make([]byte, sketch.SerializedSizeBytes(withBuffer))

	buf[offset] = sketch.preambleLongs()
	offset++

	buf[offset] = serialVersion
	offset++

	buf[offset] = uint8(internal.FamilyEnum.TDigest.Id)
	offset++

	binary.LittleEndian.PutUint16(buf[offset:], sketch.k)
	offset += 2

	var flagsByte byte
	if sketch.IsEmpty() {
		flagsByte |= 1 << serializationFlagIsEmpty
	}
	if sketch.isSingleValue() {
		flagsByte |= 1 << serializationFlagIsSingleValue
	}
	if sketch.reverseMerge {
		flagsByte |= 1 << serializationFlagReverseMerge
	}
	buf[offset] = flagsByte
	offset++

	// 2 bytes unused
	offset += 2

	if sketch.IsEmpty() {
		return buf, nil
	}

	if sketch.isSingleValue() {
		binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(sketch.min))

		return buf, nil
	}

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(sketch.centroids)))
	offset += 4

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(sketch.buffer)))
	offset += 4

	binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(sketch.min))
	offset += 8

	binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(sketch.max))
	offset += 8

	for _, c := range sketch.centroids {
		binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(c.mean))
		offset += 8
		binary.LittleEndian.PutUint64(buf[offset:], c.weight)
		offset += 8
	}

	if len(sketch.buffer) > 0 {
		for _, v := range sketch.buffer {
			binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(v))
			offset += 8
		}
	}

	return buf, nil
}
