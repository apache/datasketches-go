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
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/apache/datasketches-go/internal"
)

type compactResult struct {
	deltaRetItems int
	deltaNomSize  int
}

type compactor struct {
	items                  []float32
	count                  int
	isHighRankAccuracyMode bool
	sorted                 bool
	numSections            byte
	coin                   bool
	sectionSizeFlt         float32
	state                  int64
	sectionSize            int
	delta                  int
	lgWeight               byte
}

func newCompactor(lgWeight byte, hra bool, sectionSize int) *compactor {
	c := &compactor{
		lgWeight:               lgWeight,
		isHighRankAccuracyMode: hra,
		sectionSize:            sectionSize,
		sectionSizeFlt:         float32(sectionSize),
		state:                  0,
		coin:                   false,
		numSections:            initNumberOfSections,
		sorted:                 true,
		count:                  0,
	}
	nomCap := c.NomCapacity()
	c.delta = nomCap
	c.items = make([]float32, 2*nomCap)
	return c
}

func copyCompactor(other *compactor) *compactor {
	itemsCopy := make([]float32, cap(other.items))
	copy(itemsCopy, other.items)
	return &compactor{
		lgWeight:               other.lgWeight,
		isHighRankAccuracyMode: other.isHighRankAccuracyMode,
		sectionSizeFlt:         other.sectionSizeFlt,
		numSections:            other.numSections,
		sectionSize:            other.sectionSize,
		state:                  other.state,
		coin:                   other.coin,
		delta:                  other.delta,
		sorted:                 other.sorted,
		items:                  itemsCopy,
		count:                  other.count,
	}
}

func numberOfTrailingOnes(v int64) int {
	return bits.TrailingZeros64(uint64(^v))
}

func (c *compactor) Compact(next *compactor) (compactResult, error) {
	startRetItems := c.Count()
	startNomCap := c.NomCapacity()

	// choose to compact
	secsToCompact := numberOfTrailingOnes(c.state) + 1
	if secsToCompact > int(c.numSections) {
		secsToCompact = int(c.numSections)
	}

	compactionRange := c.computeCompactionRange(secsToCompact)
	compactionStart := int(compactionRange & 0xFFFF_FFFF) // low 32
	compactionEnd := int(compactionRange >> 32)           // high 32
	if compactionEnd-compactionStart < 2 {
		return compactResult{}, errors.New("compaction ranges too small")
	}

	if (c.state & 1) == 1 {
		c.coin = !c.coin // if numCompactions odd, flip coin
	} else {
		c.coin = rand.IntN(2) == 1 // random coin flip
	}

	promoteCount, err := c.promoteEvensOrOddsInto(next, compactionStart, compactionEnd, c.coin)
	if err != nil {
		return compactResult{}, err
	}

	c.TrimCount(c.Count() - (compactionEnd - compactionStart))
	c.state++
	c.ensureEnoughSections()

	return compactResult{
		deltaRetItems: (c.Count() - startRetItems) + promoteCount,
		deltaNomSize:  c.NomCapacity() - startNomCap,
	}, nil
}

func (c *compactor) Coin() bool {
	return c.coin
}

func (c *compactor) NomCapacity() int {
	return nomCapMul * int(c.numSections) * c.sectionSize
}

// SerializationBytes returns the number of bytes required to serialize this compactor.
// Format: state(8) + sectionSizeFlt(4) + lgWeight(1) + numSections(1) + pad(2) + count(4) + floatArr
func (c *compactor) SerializationBytes() int {
	return 8 + 4 + 1 + 1 + 2 + 4 + (c.Count() * 4) // 20 + array
}

func (c *compactor) NumSections() int {
	return int(c.numSections)
}

func (c *compactor) SectionSize() int {
	return c.sectionSize
}

func (c *compactor) SectionSizeFlt() float32 {
	return c.sectionSizeFlt
}

func (c *compactor) State() int64 {
	return c.state
}

func (c *compactor) IsHighRankAccuracyMode() bool {
	return c.isHighRankAccuracyMode
}

func (c *compactor) Merge(other *compactor) error {
	if c.lgWeight != other.lgWeight {
		return fmt.Errorf("compaction weights do not match. current: %d, other: %d", c.lgWeight, other.lgWeight)
	}

	c.state |= other.state
	for c.ensureEnoughSections() {
	}

	c.Sort()

	otherCopy := copyCompactor(other)
	otherCopy.Sort()
	if otherCopy.Count() > c.Count() {
		c.items, otherCopy.items = otherCopy.items, c.items
		c.count, otherCopy.count = otherCopy.count, c.count
		c.delta, otherCopy.delta = otherCopy.delta, c.delta
	}
	if err := c.mergeSortIn(otherCopy); err != nil {
		return err
	}
	return nil
}

func (c *compactor) ensureEnoughSections() bool {
	if c.state >= (int64(1)<<(c.numSections-1)) && c.sectionSize > minK {
		szf := float32(float64(c.sectionSizeFlt) / math.Sqrt2)
		ne := nearestEven(szf)
		if ne >= minK {
			c.sectionSizeFlt = szf
			c.sectionSize = ne
			c.numSections <<= 1
			c.ensureCapacity(2 * c.NomCapacity())
			return true
		}
	}
	return false
}

func (c *compactor) computeCompactionRange(secsToCompact int) int64 {
	cnt := c.Count()
	nonCompact := (c.NomCapacity() / 2) + (int(c.numSections)-secsToCompact)*c.sectionSize
	if (cnt-nonCompact)&1 == 1 {
		nonCompact++
	}
	var low, high int64
	if c.isHighRankAccuracyMode {
		low = 0
		high = int64(cnt - nonCompact)
	} else {
		low = int64(nonCompact)
		high = int64(cnt)
	}
	return (high << 32) + low
}

func nearestEven(val float32) int {
	return int(math.Round(float64(val)/2.0)) << 1
}

func (c *compactor) MarshalBinary() ([]byte, error) {
	size := c.SerializationBytes()
	arr := make([]byte, size)
	offset := 0

	binary.LittleEndian.PutUint64(arr[offset:], uint64(c.state))
	offset += 8

	binary.LittleEndian.PutUint32(arr[offset:], math.Float32bits(c.sectionSizeFlt))
	offset += 4

	arr[offset] = c.lgWeight
	offset++
	arr[offset] = c.numSections
	offset++

	offset += 2 // pad

	binary.LittleEndian.PutUint32(arr[offset:], uint32(c.Count()))
	offset += 4

	floatBytes := c.marshalItems()
	copy(arr[offset:], floatBytes)
	return arr, nil
}

func (c *compactor) String() string {
	return fmt.Sprintf(
		"  C:%d Len:%d NomSz:%d SecSz:%d NumSec:%d State:%d",
		c.lgWeight, c.Count(), c.NomCapacity(),
		c.sectionSize, c.numSections, c.state,
	)
}

func (c *compactor) Append(item float32) {
	c.ensureSpace(1)
	var index int
	if c.isHighRankAccuracyMode {
		index = cap(c.items) - c.count - 1
	} else {
		index = c.count
	}
	c.items[index] = item
	c.count++
	c.sorted = false
}

func (c *compactor) Count() int {
	return c.count
}

// Item gets an item given its offset in the active region.
func (c *compactor) Item(offset int) float32 {
	var index int
	if c.isHighRankAccuracyMode {
		index = cap(c.items) - c.count + offset
	} else {
		index = offset
	}
	return c.items[index]
}

func (c *compactor) Capacity() int {
	return cap(c.items)
}

// countWithCriterion returns the count of items satisfying the given search criterion.
// NOTE: used for test.
func (c *compactor) countWithCriterion(item float32, inclusive bool) (int, error) {
	if math.IsNaN(float64(item)) {
		return 0, errors.New("float items must not be NaN")
	}

	c.Sort()

	low := 0
	high := c.count - 1
	if c.isHighRankAccuracyMode {
		capacity := cap(c.items)
		low = capacity - c.count
		high = capacity - 1
	}

	crit := internal.InequalityLT
	if inclusive {
		crit = internal.InequalityLE
	}

	index, err := internal.FindWithInequality[float32](
		c.items,
		low,
		high,
		item,
		crit,
		func(f1 float32, f2 float32) bool {
			return f1 < f2
		},
	)
	if err != nil {
		return 0, err
	}

	if index == -1 {
		return 0, nil
	}
	return index - low + 1, nil
}

func (c *compactor) Sort() {
	if c.sorted {
		return
	}

	var start, end int
	if c.isHighRankAccuracyMode {
		capacity := cap(c.items)
		start = capacity - c.count
		end = capacity
	} else {
		start = 0
		end = c.count
	}
	sub := c.items[start:end]
	slices.Sort(sub)
	c.sorted = true
}

func (c *compactor) marshalItems() []byte {
	out := make([]byte, 4*c.count)
	var start int
	if c.isHighRankAccuracyMode {
		start = cap(c.items) - c.count
	}

	for i := 0; i < c.count; i++ {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(c.items[start+i]))
	}
	return out
}

// itemsToString returns a formatted string of the items.
func (c *compactor) itemsToString(width int) string {
	var sb strings.Builder
	spaces := "  "
	var start, end int
	if c.isHighRankAccuracyMode {
		capacity := cap(c.items)
		start = capacity - c.count
		end = capacity
	} else {
		end = c.count
	}

	cnt := 0
	sb.WriteString(spaces)
	for i := start; i < end; i++ {
		str := fmt.Sprintf("%f", c.items[i])
		if i > start {
			cnt++
			if cnt%width == 0 {
				sb.WriteString("\n")
				sb.WriteString(spaces)
			}
		}
		sb.WriteString(str)
	}
	return sb.String()
}

func (c *compactor) TrimCapacity() {
	capacity := cap(c.items)
	if c.count < capacity {
		out := make([]float32, c.count)
		var start int
		if c.isHighRankAccuracyMode {
			start = capacity - c.count
		}
		copy(out, c.items[start:start+c.count])
		c.items = out
	}
}

func (c *compactor) mergeSortIn(other *compactor) error {
	if !c.sorted || !other.sorted {
		return errors.New("both compactors must be sorted")
	}

	c.ensureSpace(other.count)
	totLen := c.count + other.count

	if c.isHighRankAccuracyMode {
		currCap := cap(c.items)
		otherCap := cap(other.items)

		tgtStart := currCap - totLen
		i := currCap - c.count
		j := otherCap - other.count
		for k := tgtStart; k < currCap; k++ {
			if i < currCap && j < otherCap {
				if c.items[i] <= other.items[j] {
					c.items[k] = c.items[i]
					i++
				} else {
					c.items[k] = other.items[j]
					j++
				}
			} else if i < currCap {
				c.items[k] = c.items[i]
				i++
			} else if j < otherCap {
				c.items[k] = other.items[j]
				j++
			} else {
				break
			}
		}
	} else {
		i := c.count - 1
		j := other.count - 1
		for k := totLen - 1; k >= 0; k-- {
			if i >= 0 && j >= 0 {
				if c.items[i] >= other.items[j] {
					c.items[k] = c.items[i]
					i--
				} else {
					c.items[k] = other.items[j]
					j--
				}
			} else if i >= 0 {
				c.items[k] = c.items[i]
				i--
			} else if j >= 0 {
				c.items[k] = other.items[j]
				j--
			} else {
				break
			}
		}
	}

	c.count += other.count
	c.sorted = true
	return nil
}

func (c *compactor) promoteEvensOrOddsInto(next *compactor, startOffset, endOffset int, odds bool) (int, error) {
	var start, end int
	if c.isHighRankAccuracyMode {
		capacity := cap(c.items)
		start = (capacity - c.count) + startOffset
		end = (capacity - c.count) + endOffset
	} else {
		start = startOffset
		end = endOffset
	}

	c.Sort()

	rangeSize := endOffset - startOffset
	if rangeSize&1 == 1 {
		return 0, errors.New("input range size must be even")
	}

	odd := 0
	if odds {
		odd = 1
	}

	promoteCount := rangeSize / 2
	promoted := make([]float32, promoteCount)
	j := 0
	for i := start + odd; i < end; i += 2 {
		promoted[j] = c.items[i]
		j++
	}

	promotedCompactor := &compactor{
		items:  promoted,
		count:  promoteCount,
		sorted: true,
	}
	if err := next.mergeSortIn(promotedCompactor); err != nil {
		return 0, err
	}

	return promoteCount, nil
}

func (c *compactor) TrimCount(newCount int) {
	if newCount < c.count {
		c.count = newCount
	}
}

func (c *compactor) ensureSpace(space int) {
	if (c.count + space) > cap(c.items) {
		newCap := c.count + space + c.delta
		c.ensureCapacity(newCap)
	}
}

func (c *compactor) ensureCapacity(newCapacity int) {
	oldCapacity := cap(c.items)
	if newCapacity > oldCapacity {
		out := make([]float32, newCapacity)
		var srcPos, destPos int
		if c.isHighRankAccuracyMode {
			srcPos = oldCapacity - c.count
			destPos = newCapacity - c.count
		}
		copy(out[destPos:destPos+c.count], c.items[srcPos:srcPos+c.count])
		c.items = out
	}
}

type compactorDecodingResult struct {
	compactor      *compactor
	bufferEndIndex int
	minItem        float32
	maxItem        float32
	n              int64
}

func decodeCompactor(
	buf []byte, index int, isLevel0Sorted, isHighRankAccuracyMode bool,
) (compactorDecodingResult, error) { // the second returned value is the end index of after decoding.
	if err := validateBuffer(buf, index+8); err != nil {
		return compactorDecodingResult{}, err
	}
	state := binary.LittleEndian.Uint64(buf[index : index+8])
	index += 8

	if err := validateBuffer(buf, index+4); err != nil {
		return compactorDecodingResult{}, err
	}
	sectionSizeFloat := math.Float32frombits(binary.LittleEndian.Uint32(buf[index : index+4]))
	sectionSize := math.Round(float64(sectionSizeFloat))
	index += 4

	if err := validateBuffer(buf, index+1); err != nil {
		return compactorDecodingResult{}, err
	}
	lgWeight := buf[index]
	index++

	if err := validateBuffer(buf, index+1); err != nil {
		return compactorDecodingResult{}, err
	}
	numSections := buf[index]
	index++

	index += 2 // pad

	if err := validateBuffer(buf, index+4); err != nil {
		return compactorDecodingResult{}, err
	}
	count := binary.LittleEndian.Uint32(buf[index : index+4])
	index += 4

	var (
		minItem = float32(math.MaxFloat32)
		maxItem = float32(-math.MaxFloat32)
	)
	items := make([]float32, 0, count)
	for i := uint32(0); i < count; i++ {
		if err := validateBuffer(buf, index+4); err != nil {
			return compactorDecodingResult{}, err
		}

		item := math.Float32frombits(binary.LittleEndian.Uint32(buf[index : index+4]))
		items = append(items, item)

		minItem = min(minItem, item)
		maxItem = max(maxItem, item)

		index += 4
	}

	delta := 2 * int(sectionSize) * int(numSections)
	nomCap := 2 * delta
	capacity := max(int(count), nomCap)

	if isHighRankAccuracyMode {
		newItems := make([]float32, capacity)
		copy(newItems[capacity-int(count):], items)
		items = newItems
	}

	return compactorDecodingResult{
		compactor: &compactor{
			lgWeight:               lgWeight,
			items:                  items,
			count:                  int(count),
			delta:                  delta,
			sorted:                 isLevel0Sorted,
			isHighRankAccuracyMode: isHighRankAccuracyMode,
			sectionSizeFlt:         sectionSizeFloat,
			numSections:            numSections,
			state:                  int64(state),
			coin:                   false,
			sectionSize:            nearestEven(sectionSizeFloat),
		},
		bufferEndIndex: index,
		minItem:        minItem,
		maxItem:        maxItem,
		n:              int64(count),
	}, nil
}

type compactorDecoder struct {
	isLevel0Sorted         bool
	isHighRankAccuracyMode bool
}

func newCompactorDecoder(isLevel0Sorted, isHighRankAccuracyMode bool) compactorDecoder {
	return compactorDecoder{
		isLevel0Sorted:         isLevel0Sorted,
		isHighRankAccuracyMode: isHighRankAccuracyMode,
	}
}

func (d *compactorDecoder) Decode(r io.Reader) (compactorDecodingResult, error) {
	var state uint64
	if err := binary.Read(r, binary.LittleEndian, &state); err != nil {
		return compactorDecodingResult{}, err
	}

	var sectionSizeFltRaw uint32
	if err := binary.Read(r, binary.LittleEndian, &sectionSizeFltRaw); err != nil {
		return compactorDecodingResult{}, err
	}
	sectionSizeFlt := math.Float32frombits(sectionSizeFltRaw)
	sectionSize := math.Round(float64(sectionSizeFlt))

	var lgWeight byte
	if err := binary.Read(r, binary.LittleEndian, &lgWeight); err != nil {
		return compactorDecodingResult{}, err
	}

	var numSections byte
	if err := binary.Read(r, binary.LittleEndian, &numSections); err != nil {
		return compactorDecodingResult{}, err
	}

	var pad uint16
	if err := binary.Read(r, binary.LittleEndian, &pad); err != nil {
		return compactorDecodingResult{}, err
	}

	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return compactorDecodingResult{}, err
	}

	var (
		minItem = float32(math.MaxFloat32)
		maxItem = float32(-math.MaxFloat32)
	)
	items := make([]float32, 0, count)
	for i := uint32(0); i < count; i++ {
		var itemRaw uint32
		if err := binary.Read(r, binary.LittleEndian, &itemRaw); err != nil {
			return compactorDecodingResult{}, err
		}

		item := math.Float32frombits(itemRaw)
		items = append(items, item)

		minItem = min(minItem, item)
		maxItem = max(maxItem, item)
	}

	delta := 2 * int(sectionSize) * int(numSections)
	nomCap := 2 * delta
	capacity := max(int(count), nomCap)

	if d.isHighRankAccuracyMode {
		newItems := make([]float32, capacity)
		copy(newItems[capacity-int(count):], items)
		items = newItems
	}

	return compactorDecodingResult{
		compactor: &compactor{
			lgWeight:               lgWeight,
			items:                  items,
			count:                  int(count),
			delta:                  delta,
			sorted:                 d.isLevel0Sorted,
			isHighRankAccuracyMode: d.isHighRankAccuracyMode,
			sectionSizeFlt:         sectionSizeFlt,
			numSections:            numSections,
			state:                  int64(state),
			coin:                   false,
			sectionSize:            nearestEven(sectionSizeFlt),
		},
		minItem: minItem,
		maxItem: maxItem,
		n:       int64(count),
	}, nil
}
