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
	"errors"
	"io"
	"math"

	"github.com/apache/datasketches-go/internal"
)

var (
	errSketchTypeMismatch    = errors.New("sketch type mismatch")
	errSerialVersionMismatch = errors.New("serial version mismatch")
	errPreambleMismatch      = errors.New("preamble longs mismatch")
	errInsufficientData      = errors.New("insufficient data for deserialization")
	errUnexpectedPreamble    = errors.New("unexpected sketch preamble")
)

// DoubleDecoder decodes a Double from the given reader.
type DoubleDecoder struct{}

// NewDoubleDecoder creates and returns a new instance of DoubleDecoder.
func NewDoubleDecoder() DoubleDecoder {
	return DoubleDecoder{}
}

// Decode decodes t-Digest double precision type from a given stream.
func (d *DoubleDecoder) Decode(r io.Reader) (*Double, error) {
	var preambleLongs uint8
	if err := binary.Read(r, binary.LittleEndian, &preambleLongs); err != nil {
		return nil, err
	}

	var serialVer uint8
	if err := binary.Read(r, binary.LittleEndian, &serialVer); err != nil {
		return nil, err
	}

	var skType uint8
	if err := binary.Read(r, binary.LittleEndian, &skType); err != nil {
		return nil, err
	}

	if skType != uint8(internal.FamilyEnum.TDigest.Id) {
		if preambleLongs == 0 && serialVer == 0 && skType == 0 {
			return d.decodeCompat(r)
		}
		return nil, errSketchTypeMismatch
	}
	if serialVer != serialVersion {
		return nil, errSerialVersionMismatch
	}

	var k uint16
	if err := binary.Read(r, binary.LittleEndian, &k); err != nil {
		return nil, err
	}

	var flagsByte uint8
	if err := binary.Read(r, binary.LittleEndian, &flagsByte); err != nil {
		return nil, err
	}

	isEmpty := flagsByte&(1<<serializationFlagIsEmpty) != 0
	isSingleValue := flagsByte&(1<<serializationFlagIsSingleValue) != 0
	reverseMerge := flagsByte&(1<<serializationFlagReverseMerge) != 0

	expectedPreambleLongs := preambleLongsMultiple
	if isEmpty || isSingleValue {
		expectedPreambleLongs = preambleLongsEmptyOrSingle
	}
	if preambleLongs != expectedPreambleLongs {
		return nil, errPreambleMismatch
	}

	var unused uint16
	if err := binary.Read(r, binary.LittleEndian, &unused); err != nil {
		return nil, err
	}

	if isEmpty {
		return NewDouble(k)
	}

	if isSingleValue {
		var valueBytes uint64
		if err := binary.Read(r, binary.LittleEndian, &valueBytes); err != nil {
			return nil, err
		}

		value := math.Float64frombits(valueBytes)
		if err := validateNaN(value, "single_value"); err != nil {
			return nil, err
		}
		if err := validateInf(value, "single_value"); err != nil {
			return nil, err
		}
		return newDoubleFromInternalStates(reverseMerge, k, value, value, []doublePrecisionCentroid{{mean: value, weight: 1}}, 1, nil)
	}

	var numCentroids uint32
	if err := binary.Read(r, binary.LittleEndian, &numCentroids); err != nil {
		return nil, err
	}

	var numBuffered uint32
	if err := binary.Read(r, binary.LittleEndian, &numBuffered); err != nil {
		return nil, err
	}

	var minValBytes uint64
	if err := binary.Read(r, binary.LittleEndian, &minValBytes); err != nil {
		return nil, err
	}
	minVal := math.Float64frombits(minValBytes)
	if err := validateNaN(minVal, "min"); err != nil {
		return nil, err
	}

	var maxValBytes uint64
	if err := binary.Read(r, binary.LittleEndian, &maxValBytes); err != nil {
		return nil, err
	}
	maxVal := math.Float64frombits(maxValBytes)
	if err := validateNaN(minVal, "max"); err != nil {
		return nil, err
	}

	centroids := make([]doublePrecisionCentroid, numCentroids)
	var totalWeight uint64
	for i := range centroids {
		var meanBytes uint64
		if err := binary.Read(r, binary.LittleEndian, &meanBytes); err != nil {
			return nil, err
		}
		mean := math.Float64frombits(meanBytes)
		if err := validateNaN(mean, "centroid mean"); err != nil {
			return nil, err
		}
		if err := validateInf(mean, "centroid mean"); err != nil {
			return nil, err
		}
		centroids[i].mean = mean

		var weightBytes uint64
		if err := binary.Read(r, binary.LittleEndian, &weightBytes); err != nil {
			return nil, err
		}
		weight := math.Float64bits(math.Float64frombits(weightBytes))
		if err := validateZero(float64(weight), "centroid weight"); err != nil {
			return nil, err
		}

		centroids[i].weight = weight
		totalWeight += weight
	}

	buffer := make([]float64, numBuffered)
	for i := range buffer {
		var dataBytes uint64
		if err := binary.Read(r, binary.LittleEndian, &dataBytes); err != nil {
			return nil, err
		}

		bufferedValue := math.Float64frombits(dataBytes)
		if err := validateNaN(bufferedValue, "buffered value"); err != nil {
			return nil, err
		}
		if err := validateInf(bufferedValue, "buffered value"); err != nil {
			return nil, err
		}

		buffer[i] = bufferedValue
	}

	return newDoubleFromInternalStates(reverseMerge, k, minVal, maxVal, centroids, totalWeight, buffer)
}

func (d *DoubleDecoder) decodeCompat(r io.Reader) (*Double, error) {
	var typeFlag uint8
	if err := binary.Read(r, binary.BigEndian, &typeFlag); err != nil {
		return nil, err
	}

	if typeFlag == compatTypeDouble { // compatibility with asBytes()
		var minValBytes uint64
		if err := binary.Read(r, binary.BigEndian, &minValBytes); err != nil {
			return nil, err
		}
		minVal := math.Float64frombits(minValBytes)
		if err := validateNaN(minVal, "min"); err != nil {
			return nil, err
		}

		var maxValBytes uint64
		if err := binary.Read(r, binary.BigEndian, &maxValBytes); err != nil {
			return nil, err
		}
		maxVal := math.Float64frombits(maxValBytes)
		if err := validateNaN(maxVal, "max"); err != nil {
			return nil, err
		}

		var kBytes uint64
		if err := binary.Read(r, binary.BigEndian, &kBytes); err != nil {
			return nil, err
		}
		k := uint16(math.Float64frombits(kBytes))

		var numCentroids uint32
		if err := binary.Read(r, binary.BigEndian, &numCentroids); err != nil {
			return nil, err
		}

		centroids := make([]doublePrecisionCentroid, numCentroids)
		var totalWeight uint64
		for i := range centroids {
			var weightBytes uint64
			if err := binary.Read(r, binary.BigEndian, &weightBytes); err != nil {
				return nil, err
			}
			weight := math.Float64frombits(weightBytes)
			if err := validateZero(weight, "centroid weight"); err != nil {
				return nil, err
			}

			var meanBytes uint64
			if err := binary.Read(r, binary.BigEndian, &meanBytes); err != nil {
				return nil, err
			}
			mean := math.Float64frombits(meanBytes)
			if err := validateNaN(mean, "centroid mean"); err != nil {
				return nil, err
			}
			if err := validateInf(mean, "centroid mean"); err != nil {
				return nil, err
			}

			centroids[i] = doublePrecisionCentroid{mean: mean, weight: uint64(weight)}
			totalWeight += uint64(weight)
		}

		return newDoubleFromInternalStates(false, k, minVal, maxVal, centroids, totalWeight, nil)
	}

	var minValBytes uint64
	if err := binary.Read(r, binary.BigEndian, &minValBytes); err != nil {
		return nil, err
	}
	minVal := math.Float64frombits(minValBytes)
	if err := validateNaN(minVal, "min"); err != nil {
		return nil, err
	}

	var maxValBytes uint64
	if err := binary.Read(r, binary.BigEndian, &maxValBytes); err != nil {
		return nil, err
	}
	maxVal := math.Float64frombits(maxValBytes)
	if err := validateNaN(maxVal, "max"); err != nil {
		return nil, err
	}

	var kBytes uint32
	if err := binary.Read(r, binary.BigEndian, &kBytes); err != nil {
		return nil, err
	}
	k := uint16(math.Float32frombits(kBytes))

	var unused uint32
	if err := binary.Read(r, binary.BigEndian, &unused); err != nil {
		return nil, err
	}

	var numCentroids uint16
	if err := binary.Read(r, binary.BigEndian, &numCentroids); err != nil {
		return nil, err
	}

	centroids := make([]doublePrecisionCentroid, numCentroids)
	var totalWeight uint64
	for i := range centroids {
		var weightBytes uint32
		if err := binary.Read(r, binary.BigEndian, &weightBytes); err != nil {
			return nil, err
		}
		weight := math.Float32frombits(weightBytes)
		if err := validateZero(float64(weight), "centroid weight"); err != nil {
			return nil, err
		}

		var meanBytes uint32
		if err := binary.Read(r, binary.BigEndian, &meanBytes); err != nil {
			return nil, err
		}
		mean := math.Float32frombits(meanBytes)
		if err := validateInf(float64(mean), "centroid mean"); err != nil {
			return nil, err
		}
		if err := validateNaN(float64(weight), "centroid mean"); err != nil {
			return nil, err
		}

		centroids[i] = doublePrecisionCentroid{mean: float64(mean), weight: uint64(weight)}
		totalWeight += uint64(weight)
	}

	return newDoubleFromInternalStates(false, k, minVal, maxVal, centroids, totalWeight, nil)
}

// DecodeDouble deserializes bytes into a Double.
func DecodeDouble(data []byte) (*Double, error) {
	if len(data) < 8 {
		return nil, errInsufficientData
	}

	offset := 0

	preambleLongs := data[offset]
	offset++

	serialVer := data[offset]
	offset++

	skType := data[offset]
	offset++

	if skType != uint8(internal.FamilyEnum.TDigest.Id) {
		if preambleLongs == 0 && serialVer == 0 && skType == 0 {
			return decodeDoubleCompat(data[offset:])
		}
		return nil, errSketchTypeMismatch
	}
	if serialVer != serialVersion {
		return nil, errSerialVersionMismatch
	}

	k := binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	flagsByte := data[offset]
	offset++

	isEmpty := flagsByte&(1<<serializationFlagIsEmpty) != 0
	isSingleValue := flagsByte&(1<<serializationFlagIsSingleValue) != 0
	reverseMerge := flagsByte&(1<<serializationFlagReverseMerge) != 0

	expectedPreambleLongs := preambleLongsMultiple
	if isEmpty || isSingleValue {
		expectedPreambleLongs = preambleLongsEmptyOrSingle
	}
	if preambleLongs != expectedPreambleLongs {
		return nil, errPreambleMismatch
	}

	offset += 2 // unused

	if isEmpty {
		return NewDouble(k)
	}

	if isSingleValue {
		if len(data) < offset+8 {
			return nil, errInsufficientData
		}

		value := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		if err := validateNaN(value, "value"); err != nil {
			return nil, err
		}
		if err := validateInf(value, "value"); err != nil {
			return nil, err
		}
		return newDoubleFromInternalStates(reverseMerge, k, value, value, []doublePrecisionCentroid{{mean: value, weight: 1}}, 1, nil)
	}

	if len(data) < offset+8 {
		return nil, errInsufficientData
	}

	numCentroids := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	numBuffered := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	expectedSize := offset + 16 + int(numCentroids)*16 + int(numBuffered)*8
	if len(data) < expectedSize {
		return nil, errInsufficientData
	}

	minVal := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	if err := validateNaN(minVal, "min"); err != nil {
		return nil, err
	}

	maxVal := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	if err := validateNaN(maxVal, "max"); err != nil {
		return nil, err
	}

	centroids := make([]doublePrecisionCentroid, numCentroids)
	var totalWeight uint64
	for i := range centroids {
		mean := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		if err := validateNaN(mean, "centroid mean"); err != nil {
			return nil, err
		}
		if err := validateInf(mean, "centroid mean"); err != nil {
			return nil, err
		}
		centroids[i].mean = mean

		weight := binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		if err := validateZero(float64(weight), "centroid weight"); err != nil {
			return nil, err
		}

		centroids[i].weight = weight
		totalWeight += weight
	}

	buffer := make([]float64, numBuffered)
	for i := range buffer {
		bufferedValue := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		if err := validateNaN(bufferedValue, "buffered value"); err != nil {
			return nil, err
		}
		if err := validateInf(bufferedValue, "buffered value"); err != nil {
			return nil, err
		}

		buffer[i] = bufferedValue
	}

	return newDoubleFromInternalStates(reverseMerge, k, minVal, maxVal, centroids, totalWeight, buffer)
}

func decodeDoubleCompat(data []byte) (*Double, error) {
	if len(data) < 1 {
		return nil, errInsufficientData
	}

	offset := 0

	typeFlag := data[offset]
	if typeFlag != compatTypeDouble && typeFlag != compatTypeFloat {
		return nil, errUnexpectedPreamble
	}
	offset++

	if typeFlag == compatTypeDouble { // compatibility with asBytes()
		if len(data) < offset+28 { // 3 doubles + 1 uint32
			return nil, errInsufficientData
		}

		minVal := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		if err := validateNaN(minVal, "min"); err != nil {
			return nil, err
		}

		maxVal := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		if err := validateNaN(maxVal, "max"); err != nil {
			return nil, err
		}

		kDouble := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
		k := uint16(kDouble)
		offset += 8

		numCentroids := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		if len(data) < offset+int(numCentroids)*16 {
			return nil, errInsufficientData
		}

		centroids := make([]doublePrecisionCentroid, numCentroids)
		var totalWeight uint64
		for i := range centroids {
			weight := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
			offset += 8
			if err := validateZero(float64(weight), "centroid weight"); err != nil {
				return nil, err
			}

			mean := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
			offset += 8
			if err := validateNaN(mean, "centroid mean"); err != nil {
				return nil, err
			}
			if err := validateInf(mean, "centroid mean"); err != nil {
				return nil, err
			}

			centroids[i] = doublePrecisionCentroid{mean: mean, weight: uint64(weight)}
			totalWeight += uint64(weight)
		}

		return newDoubleFromInternalStates(false, k, minVal, maxVal, centroids, totalWeight, nil)
	}

	// compatFloat: compatibility with asSmallBytes()
	if len(data) < offset+24 { // 2 doubles + 1 float + 2 uint16 + 1 uint16
		return nil, errInsufficientData
	}

	minVal := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	if err := validateNaN(minVal, "min"); err != nil {
		return nil, err
	}

	maxVal := math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	if err := validateNaN(maxVal, "max"); err != nil {
		return nil, err
	}

	kFloat := math.Float32frombits(binary.BigEndian.Uint32(data[offset:]))
	k := uint16(kFloat)
	offset += 4

	offset += 4 // unused

	numCentroids := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	if len(data) < offset+int(numCentroids)*8 {
		return nil, errInsufficientData
	}

	centroids := make([]doublePrecisionCentroid, numCentroids)
	var totalWeight uint64
	for i := range centroids {
		weight := math.Float32frombits(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if err := validateZero(float64(weight), "centroid weight"); err != nil {
			return nil, err
		}

		mean := math.Float32frombits(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if err := validateNaN(float64(mean), "centroid mean"); err != nil {
			return nil, err
		}
		if err := validateInf(float64(mean), "centroid mean"); err != nil {
			return nil, err
		}

		centroids[i] = doublePrecisionCentroid{mean: float64(mean), weight: uint64(weight)}
		totalWeight += uint64(weight)
	}

	return newDoubleFromInternalStates(false, k, minVal, maxVal, centroids, totalWeight, nil)
}

func validateNaN(v float64, name string) error {
	if math.IsNaN(v) {
		return errors.New(name + ": NaN")
	}
	return nil
}

func validateInf(v float64, name string) error {
	if math.IsInf(v, 0) {
		return errors.New(name + ": Inf")
	}
	return nil
}

func validateZero(v float64, name string) error {
	if v == 0.0 {
		return errors.New(name + ": Zero")
	}
	return nil
}
