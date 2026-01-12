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
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
)

const (
	DefaultK         = 200
	bufferMultiplier = 4
)

const (
	preambleLongsEmptyOrSingle uint8 = 1
	preambleLongsMultiple      uint8 = 2
	serialVersion              uint8 = 1
)

const (
	compatTypeDouble uint8 = 1
	compatTypeFloat  uint8 = 2
)

const (
	serializationFlagIsEmpty uint8 = iota
	serializationFlagIsSingleValue
	serializationFlagReverseMerge
)

var (
	ErrEmpty              = errors.New("operation is undefined for an empty sketch")
	ErrNaN                = errors.New("operation is undefined for NaN")
	ErrInvalidRank        = errors.New("normalized rank must be between 0 and 1 inclusive")
	ErrInvalidK           = errors.New("k must be at least 10")
	errNanInSplitPoints   = errors.New("NaN in split points")
	errInvalidSplitPoints = errors.New("values must be unique and monotonically increasing")
)

func doublePrecisionCentroidSortFunc(c1, c2 doublePrecisionCentroid) int {
	if c1.mean < c2.mean {
		return -1
	} else if c1.mean > c2.mean {
		return 1
	}
	return 0
}

type doublePrecisionCentroid struct {
	mean   float64
	weight uint64
}

func (c *doublePrecisionCentroid) add(other doublePrecisionCentroid) {
	c.weight += other.weight
	c.mean += (other.mean - c.mean) * float64(other.weight) / float64(c.weight)
}

// scaleFunction is equivalent of K_2 (default) in the Java implementation mentioned below
// Generates cluster sizes proportional to q*(1-q).
// The use of a normalizing function results in a strictly bounded number of clusters no matter how many samples.
type scaleFunction struct{}

func (s scaleFunction) max(q, normalizer float64) float64 {
	return q * (1 - q) / normalizer
}

func (s scaleFunction) normalizer(compression, n float64) float64 {
	return compression / s.z(compression, n)
}

func (s scaleFunction) z(compression, n float64) float64 {
	return 4*math.Log(n/compression) + 24
}

// Double provides an implementation of t-Digest double precision type for estimating quantiles and ranks.
// This implementation is based on the paper:
// Ted Dunning, Otmar Ertl. "Extremely Accurate Quantiles Using t-Digests"
// and the reference implementation: https://github.com/tdunning/t-digest
// NOTE: This implementation is similar to MergingDigest in the above Java implementation
type Double struct {
	min               float64
	max               float64
	centroids         []doublePrecisionCentroid
	buffer            []float64
	centroidsWeight   uint64
	centroidsCapacity int
	k                 uint16
	reverseMerge      bool
}

// NewDouble creates a new Double with the given compression parameter k.
func NewDouble(k uint16) (*Double, error) {
	if k < 10 {
		return nil, ErrInvalidK
	}

	fudge := 10
	if k < 30 {
		fudge = 30
	}
	capacity := 2*int(k) + fudge

	return &Double{
		reverseMerge:      false,
		k:                 k,
		min:               math.Inf(1),
		max:               math.Inf(-1),
		centroidsCapacity: capacity,
		centroids:         make([]doublePrecisionCentroid, 0, capacity),
		centroidsWeight:   0,
		buffer:            make([]float64, 0, capacity*bufferMultiplier),
	}, nil
}

func newDoubleFromInternalStates(
	reverseMerge bool,
	k uint16,
	min float64,
	max float64,
	centroids []doublePrecisionCentroid,
	weight uint64,
	buffer []float64,
) (*Double, error) {
	if k < 10 {
		return nil, ErrInvalidK
	}

	fudge := 10
	if k < 30 {
		fudge = 30
	}
	capacity := 2*int(k) + fudge

	// Ensure centroids has enough capacity
	if cap(centroids) < capacity {
		newCentroids := make([]doublePrecisionCentroid, len(centroids), capacity)
		copy(newCentroids, centroids)
		centroids = newCentroids
	}

	// Ensure the buffer has enough capacity
	if buffer == nil {
		buffer = make([]float64, 0, capacity*bufferMultiplier)
	} else if cap(buffer) < capacity*bufferMultiplier {
		newBuffer := make([]float64, len(buffer), capacity*bufferMultiplier)
		copy(newBuffer, buffer)
		buffer = newBuffer
	}

	return &Double{
		reverseMerge:      reverseMerge,
		k:                 k,
		min:               min,
		max:               max,
		centroidsCapacity: capacity,
		centroids:         centroids,
		centroidsWeight:   weight,
		buffer:            buffer,
	}, nil
}

// Update updates a value to the t-Digest
func (d *Double) Update(value float64) error {
	if math.IsNaN(value) {
		return ErrNaN
	}

	if len(d.buffer) == d.centroidsCapacity*bufferMultiplier {
		d.compress()
	}
	d.buffer = append(d.buffer, value)
	d.min = math.Min(d.min, value)
	d.max = math.Max(d.max, value)

	return nil
}

// Merge merges another Double into this one
func (d *Double) Merge(other *Double) error {
	if other.IsEmpty() {
		return ErrEmpty
	}

	tmp := make([]doublePrecisionCentroid, 0, len(d.buffer)+len(d.centroids)+len(other.buffer)+len(other.centroids))
	for _, v := range d.buffer {
		tmp = append(tmp, doublePrecisionCentroid{mean: v, weight: 1})
	}
	for _, v := range other.buffer {
		tmp = append(tmp, doublePrecisionCentroid{mean: v, weight: 1})
	}
	tmp = append(tmp, other.centroids...)

	d.merge(tmp, uint64(len(d.buffer))+other.TotalWeight())

	return nil
}

func (d *Double) compress() {
	if len(d.buffer) == 0 {
		return
	}
	tmp := make([]doublePrecisionCentroid, 0, len(d.buffer)+len(d.centroids))
	for _, v := range d.buffer {
		tmp = append(tmp, doublePrecisionCentroid{mean: v, weight: 1})
	}
	d.merge(tmp, uint64(len(d.buffer)))
}

// IsEmpty returns true if the t-Digest has not seen any data
func (d *Double) IsEmpty() bool {
	return len(d.centroids) == 0 && len(d.buffer) == 0
}

// MinValue returns the minimum value seen by the t-Digest
func (d *Double) MinValue() (float64, error) {
	if d.IsEmpty() {
		return 0, ErrEmpty
	}
	return d.min, nil
}

// MaxValue returns the maximum value seen by the t-Digest
func (d *Double) MaxValue() (float64, error) {
	if d.IsEmpty() {
		return 0, ErrEmpty
	}
	return d.max, nil
}

// TotalWeight returns the total weight of all values
func (d *Double) TotalWeight() uint64 {
	return d.centroidsWeight + uint64(len(d.buffer))
}

// K returns the compression parameter k
func (d *Double) K() uint16 {
	return d.k
}

// Rank computes the approximate normalized rank of the given value
func (d *Double) Rank(value float64) (float64, error) {
	if d.IsEmpty() {
		return 0, ErrEmpty
	}
	if math.IsNaN(value) {
		return 0, ErrNaN
	}
	if value < d.min {
		return 0, nil
	}
	if value > d.max {
		return 1, nil
	}
	// one doublePrecisionCentroid and value == min == max
	if len(d.centroids)+len(d.buffer) == 1 {
		return 0.5, nil
	}

	d.compress() // side effect

	// left tail
	firstMean := d.centroids[0].mean
	if value < firstMean {
		if firstMean-d.min > 0 {
			if value == d.min {
				return 0.5 / float64(d.centroidsWeight), nil
			}
			return (1.0 + (value-d.min)/(firstMean-d.min)*(float64(d.centroids[0].weight)/2.0-1.0)) / float64(d.centroidsWeight), nil
		}
		return 0, nil // should never happen
	}

	// right tail
	lastMean := d.centroids[len(d.centroids)-1].mean
	if value > lastMean {
		if d.max-lastMean > 0 {
			if value == d.max {
				return 1.0 - 0.5/float64(d.centroidsWeight), nil
			}
			return 1.0 - ((1.0 + (d.max-value)/(d.max-lastMean)*(float64(d.centroids[len(d.centroids)-1].weight)/2.0-1.0)) / float64(d.centroidsWeight)), nil
		}
		return 1, nil // should never happen
	}

	lowerIdx := sort.Search(len(d.centroids), func(i int) bool {
		return d.centroids[i].mean >= value
	})
	if lowerIdx == len(d.centroids) {
		return 0, errors.New("value is greater than all centroids")
	}

	upperIdx := sort.Search(len(d.centroids), func(i int) bool {
		return d.centroids[i].mean > value
	})
	if upperIdx == 0 {
		return 0, errors.New("value is smaller than all centroids")
	}

	if value < d.centroids[lowerIdx].mean && lowerIdx > 0 {
		lowerIdx--
	}
	if upperIdx == len(d.centroids) || !(d.centroids[upperIdx-1].mean < value) {
		upperIdx--
	}

	var weightBelow float64
	for i := 0; i < lowerIdx; i++ {
		weightBelow += float64(d.centroids[i].weight)
	}
	weightBelow += float64(d.centroids[lowerIdx].weight) / 2.0

	var weightDelta float64
	for i := lowerIdx; i < upperIdx; i++ {
		weightDelta += float64(d.centroids[i].weight)
	}
	weightDelta -= float64(d.centroids[lowerIdx].weight) / 2.0
	weightDelta += float64(d.centroids[upperIdx].weight) / 2.0

	if d.centroids[upperIdx].mean-d.centroids[lowerIdx].mean > 0 {
		return (weightBelow + weightDelta*(value-d.centroids[lowerIdx].mean)/(d.centroids[upperIdx].mean-d.centroids[lowerIdx].mean)) / float64(d.centroidsWeight), nil
	}
	return (weightBelow + weightDelta/2.0) / float64(d.centroidsWeight), nil
}

// Quantile computes the approximate quantile value corresponding to the given normalized rank
func (d *Double) Quantile(rank float64) (float64, error) {
	if d.IsEmpty() {
		return 0, ErrEmpty
	}
	if rank < 0.0 || rank > 1.0 {
		return 0, ErrInvalidRank
	}

	d.compress() // side effect

	if len(d.centroids) == 1 {
		return d.centroids[0].mean, nil
	}

	// at least 2 centroids
	weight := rank * float64(d.centroidsWeight)
	if weight < 1 {
		return d.min, nil
	}
	if weight > float64(d.centroidsWeight)-1.0 {
		return d.max, nil
	}

	firstWeight := float64(d.centroids[0].weight)
	if firstWeight > 1 && weight < firstWeight/2.0 {
		return d.min + (weight-1.0)/(firstWeight/2.0-1.0)*(d.centroids[0].mean-d.min), nil
	}

	lastWeight := float64(d.centroids[len(d.centroids)-1].weight)
	if lastWeight > 1 && float64(d.centroidsWeight)-weight <= lastWeight/2.0 {
		return d.max + (float64(d.centroidsWeight)-weight-1.0)/(lastWeight/2.0-1.0)*(d.max-d.centroids[len(d.centroids)-1].mean), nil
	}

	// interpolate between extremes
	weightSoFar := firstWeight / 2.0
	for i := 0; i < len(d.centroids)-1; i++ {
		dw := (float64(d.centroids[i].weight) + float64(d.centroids[i+1].weight)) / 2.0
		if weightSoFar+dw > weight {
			// the target weight is between centroids i and i+1
			var leftWeight float64
			if d.centroids[i].weight == 1 {
				if weight-weightSoFar < 0.5 {
					return d.centroids[i].mean, nil
				}
				leftWeight = 0.5
			}
			var rightWeight float64
			if d.centroids[i+1].weight == 1 {
				if weightSoFar+dw-weight <= 0.5 {
					return d.centroids[i+1].mean, nil
				}
				rightWeight = 0.5
			}
			w1 := weight - weightSoFar - leftWeight
			w2 := weightSoFar + dw - weight - rightWeight
			return weightedAverage(d.centroids[i].mean, w1, d.centroids[i+1].mean, w2), nil
		}
		weightSoFar += dw
	}

	w1 := weight - float64(d.centroidsWeight) - float64(d.centroids[len(d.centroids)-1].weight)/2.0
	w2 := float64(d.centroids[len(d.centroids)-1].weight)/2.0 - w1
	return weightedAverage(d.centroids[len(d.centroids)-1].mean, w1, d.max, w2), nil
}

// PMF returns an approximation to the Probability Mass Function (PMF)
// of the input stream.
func (d *Double) PMF(splitPoints []float64) ([]float64, error) {
	buckets, err := d.CDF(splitPoints)
	if err != nil {
		return nil, err
	}
	for i := len(splitPoints); i > 0; i-- {
		buckets[i] -= buckets[i-1]
	}
	return buckets, nil
}

// CDF returns an approximation to the Cumulative Distribution Function (CDF)
// which is the cumulative analog of the PMF of the input stream.
func (d *Double) CDF(splitPoints []float64) ([]float64, error) {
	if err := validateSplitPoints(splitPoints); err != nil {
		return nil, err
	}
	ranks := make([]float64, 0, len(splitPoints)+1)
	for _, sp := range splitPoints {
		rank, err := d.Rank(sp)
		if err != nil {
			return nil, err
		}

		ranks = append(ranks, rank)
	}

	ranks = append(ranks, 1)
	return ranks, nil
}

// String returns a human-readable summary of the t-Digest
func (d *Double) String(shouldPrintCentroids bool) string {
	var sb strings.Builder
	sb.WriteString("### t-Digest summary:\n")
	sb.WriteString(fmt.Sprintf("   Nominal k          : %d\n", d.k))
	sb.WriteString(fmt.Sprintf("   Centroids          : %d\n", len(d.centroids)))
	sb.WriteString(fmt.Sprintf("   Buffered           : %d\n", len(d.buffer)))
	sb.WriteString(fmt.Sprintf("   Centroids capacity : %d\n", d.centroidsCapacity))
	sb.WriteString(fmt.Sprintf("   Buffer capacity    : %d\n", d.centroidsCapacity*bufferMultiplier))
	sb.WriteString(fmt.Sprintf("   Centroids Weight   : %d\n", d.centroidsWeight))
	sb.WriteString(fmt.Sprintf("   Total Weight       : %d\n", d.TotalWeight()))
	sb.WriteString(fmt.Sprintf("   Reverse Merge      : %v\n", d.reverseMerge))
	if !d.IsEmpty() {
		sb.WriteString(fmt.Sprintf("   Min                : %v\n", d.min))
		sb.WriteString(fmt.Sprintf("   Max                : %v\n", d.max))
	}
	sb.WriteString("### End t-Digest summary\n")

	if shouldPrintCentroids {
		if len(d.centroids) > 0 {
			sb.WriteString("Centroids:\n")
			for i, c := range d.centroids {
				sb.WriteString(fmt.Sprintf("%d: %v, %d\n", i, c.mean, c.weight))
			}
		}
		if len(d.buffer) > 0 {
			sb.WriteString("Buffer:\n")
			for i, v := range d.buffer {
				sb.WriteString(fmt.Sprintf("%d: %v\n", i, v))
			}
		}
	}
	return sb.String()
}

// SerializedSizeBytes computes the serialized size in bytes of the t-Digest.
func (d *Double) SerializedSizeBytes(withBuffer bool) int {
	if !withBuffer {
		d.compress() // side effect
	}

	size := int(d.preambleLongs() * 8)
	if d.IsEmpty() {
		return size
	}
	if d.isSingleValue() {
		return size + 8 // float64
	}

	size += 16                    // min and max (2 * float64)
	size += 16 * len(d.centroids) // each doublePrecisionCentroid is float64 + uint64
	if withBuffer {
		size += 8 * len(d.buffer) // each buffered value is float64
	}
	return size
}

func (d *Double) merge(buffer []doublePrecisionCentroid, weight uint64) {
	buffer = append(buffer, d.centroids...)
	d.centroids = d.centroids[:0]

	slices.SortStableFunc(buffer, doublePrecisionCentroidSortFunc)

	if d.reverseMerge {
		for i, j := 0, len(buffer)-1; i < j; i, j = i+1, j-1 {
			buffer[i], buffer[j] = buffer[j], buffer[i]
		}
	}

	d.centroidsWeight += weight

	d.centroids = append(d.centroids, buffer[0])

	var weightSoFar float64

	sf := scaleFunction{}
	for i := 1; i < len(buffer); i++ {
		proposedWeight := float64(d.centroids[len(d.centroids)-1].weight) + float64(buffer[i].weight)
		addThis := false
		if i != 1 && i != len(buffer)-1 {
			q0 := weightSoFar / float64(d.centroidsWeight)
			q2 := (weightSoFar + proposedWeight) / float64(d.centroidsWeight)
			normalizer := sf.normalizer(2*float64(d.k), float64(d.centroidsWeight))
			addThis = proposedWeight <= float64(d.centroidsWeight)*min(sf.max(q0, normalizer), sf.max(q2, normalizer))
		}
		if addThis {
			d.centroids[len(d.centroids)-1].add(buffer[i])
		} else {
			weightSoFar += float64(d.centroids[len(d.centroids)-1].weight)
			d.centroids = append(d.centroids, buffer[i])
		}
	}

	if d.reverseMerge {
		for i, j := 0, len(d.centroids)-1; i < j; i, j = i+1, j-1 {
			d.centroids[i], d.centroids[j] = d.centroids[j], d.centroids[i]
		}
	}

	d.min = min(d.min, d.centroids[0].mean)
	d.max = max(d.max, d.centroids[len(d.centroids)-1].mean)
	d.reverseMerge = !d.reverseMerge
	d.buffer = d.buffer[:0]
}

func (d *Double) preambleLongs() uint8 {
	if d.IsEmpty() || d.isSingleValue() {
		return preambleLongsEmptyOrSingle
	}
	return preambleLongsMultiple
}

func (d *Double) isSingleValue() bool {
	return d.TotalWeight() == 1
}

func weightedAverage(x1, w1, x2, w2 float64) float64 {
	return (x1*w1 + x2*w2) / (w1 + w2)
}

func validateSplitPoints(values []float64) error {
	for i, v := range values {
		if math.IsNaN(v) {
			return errNanInSplitPoints
		}
		if i < len(values)-1 && !(v < values[i+1]) {
			return errInvalidSplitPoints
		}
	}
	return nil
}
