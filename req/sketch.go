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
	"errors"
	"fmt"
	"math"
	"strings"

	quantilecommon "github.com/apache/datasketches-go/common/quantiles"
)

const (
	minK                 = 4
	defaultK             = 12 // 1% @ 95% Confidence
	nomCapMul            = 2
	initNumberOfSections = 3
	fixRSEFactor         = 0.084
)

var (
	relRSEFactor = math.Sqrt(0.0512 / initNumberOfSections) //0.1306394529
)

var (
	ErrEmpty = errors.New("operation is undefined for an empty sketch")
)

type SketchOptionFunc func(*Sketch)

// WithHighRankAccuracyMode sets the high rank accuracy mode for the Sketch.
func WithHighRankAccuracyMode(isHRAMode bool) SketchOptionFunc {
	return func(sk *Sketch) {
		sk.isHighRankAccuracyMode = isHRAMode
	}
}

// WithK sets the k parameter for the Sketch.
func WithK(k int) SketchOptionFunc {
	return func(sk *Sketch) {
		sk.k = k
	}
}

type Sketch struct {
	n                      int64
	compactors             []*compactor
	numRetained            int
	maxNomSize             int
	minItem                float32
	maxItem                float32
	sortedView             *quantilecommon.NumericSortedView[float32]
	k                      int
	isHighRankAccuracyMode bool
}

func NewSketch(options ...SketchOptionFunc) (*Sketch, error) {
	sk := &Sketch{
		k:                      defaultK,
		isHighRankAccuracyMode: true,
	}
	for _, option := range options {
		option(sk)
	}
	if err := sk.validateK(); err != nil {
		return nil, err
	}

	sk.grow()

	return sk, nil
}

func (s *Sketch) validateK() error {
	if s.k&1 > 0 || s.k < minK || s.k > 1024 {
		return fmt.Errorf("must be even and in the range [4, 1024]: %d", s.k)
	}

	return nil
}

func (s *Sketch) grow() {
	lgWeight := len(s.compactors)
	s.compactors = append(s.compactors, newCompactor(byte(lgWeight), s.isHighRankAccuracyMode, s.k))
	s.maxNomSize = s.computeMaxNomSize()
}

// computeMaxNomSize Computes a new bound for determining when to compress the sketch
func (s *Sketch) computeMaxNomSize() int {
	capacity := 0
	for _, comp := range s.compactors {
		capacity += comp.NomCapacity()
	}
	return capacity
}

// K returns the k parameter which controls the accuracy of the sketch
// and its memory space usage.
func (s *Sketch) K() int {
	return s.k
}

// CDF is equivalent of NumericSortedView CDF function.
func (s *Sketch) CDF(splitPoints []float32, isInclusive bool) ([]float64, error) {
	if s.IsEmpty() {
		return nil, ErrEmpty
	}

	if err := s.refreshSortedView(); err != nil {
		return nil, err
	}

	buckets, err := s.sortedView.CDF(splitPoints, isInclusive)
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

// IsEmpty checks if the sketch contains no data and returns true if it is empty, otherwise false.
func (s *Sketch) IsEmpty() bool {
	return s.n == 0
}

func (s *Sketch) refreshSortedView() error {
	if s.sortedView == nil {
		if s.IsEmpty() {
			return ErrEmpty
		}

		quantiles := make([]float32, s.numRetained)
		cumWeights := make([]int64, s.numRetained)
		count := 0
		for _, comp := range s.compactors {
			weight := 1 << comp.lgWeight
			s.mergeSortIn(
				comp, quantiles, cumWeights, int64(weight), count, s.isHighRankAccuracyMode,
			)

			count += comp.Count()
		}

		if err := s.accumulateNativeRanks(len(quantiles), cumWeights); err != nil {
			return err
		}

		s.sortedView = quantilecommon.NewNumericSortedView[float32](
			quantiles, cumWeights, s.n, s.maxItem, s.minItem,
		)
	}

	return nil
}

// specially modified version of mergeSortIn.
// weight is associated weight of input.
// count is number of items inserted.
func (s *Sketch) mergeSortIn(
	comp *compactor,
	quantiles []float32,
	cumWeights []int64,
	weight int64,
	count int,
	isHighRankAccuracyMode bool,
) {
	if !comp.sorted {
		comp.Sort()
	}

	totalLength := count + comp.Count()
	i := count - 1
	j := comp.Count() - 1
	h := comp.Count() - 1
	if isHighRankAccuracyMode {
		h = comp.Capacity() - 1
	}
	for k := totalLength - 1; k >= 0; k-- {
		switch {
		case i >= 0 && j >= 0: // both valid
			if quantiles[i] >= comp.items[h] {
				quantiles[k] = quantiles[i]
				cumWeights[k] = cumWeights[i]
				i--
			} else {
				quantiles[k] = comp.items[h]
				h--
				j--
				cumWeights[k] = weight
			}
		case i >= 0: // i is valid
			quantiles[k] = quantiles[i]
			cumWeights[k] = cumWeights[i]
			i--
		case j >= 0: // j is valid
			quantiles[k] = comp.items[h]
			h--
			j--
			cumWeights[k] = weight
		default:
			break
		}
	}
}

func (s *Sketch) accumulateNativeRanks(quantilesLength int, cumWeights []int64) error {
	for i := 1; i < quantilesLength; i++ {
		cumWeights[i] += cumWeights[i-1]
	}
	if s.n > 0 && cumWeights[quantilesLength-1] != s.n {
		return errors.New("sum of weight should equal to total count")
	}

	return nil
}

// IsHighRankAccuracyMode returns whether the sketch is in high rank accuracy mode.
// If true, the high ranks are prioritized for better accuracy.
// If not, low ranks are prioritized for better accuracy.
func (s *Sketch) IsHighRankAccuracyMode() bool {
	return s.isHighRankAccuracyMode
}

// MaxItem retrieves the maximum item in the sketch. Returns an error if the sketch is empty.
func (s *Sketch) MaxItem() (float32, error) {
	if s.IsEmpty() {
		return 0, ErrEmpty
	}
	return s.maxItem, nil
}

// MinItem retrieves the minimum item in the sketch. Returns an error if the sketch is empty.
func (s *Sketch) MinItem() (float32, error) {
	if s.IsEmpty() {
		return 0, ErrEmpty
	}
	return s.minItem, nil
}

// N returns the total number of items in the sketch.
func (s *Sketch) N() int64 {
	return s.n
}

type searchCriteria struct {
	isInclusive bool
}

// SearchCriteriaOptionFunc defines a function type used to configure or modify a searchCriteria instance.
type SearchCriteriaOptionFunc func(*searchCriteria)

// WithExclusiveSearch creates a SearchCriteriaOptionFunc that sets the search criteria to be exclusive.
func WithExclusiveSearch() SearchCriteriaOptionFunc {
	return func(c *searchCriteria) {
		c.isInclusive = false
	}
}

// PMF is the equivalent of the NumericSortedView PMF function.
// the default option is inclusive.
func (s *Sketch) PMF(splitPoints []float32, opts ...SearchCriteriaOptionFunc) ([]float64, error) {
	if s.IsEmpty() {
		return nil, ErrEmpty
	}

	if err := s.refreshSortedView(); err != nil {
		return nil, err
	}

	options := &searchCriteria{
		isInclusive: true,
	}
	for _, opt := range opts {
		opt(options)
	}

	return s.sortedView.PMF(splitPoints, options.isInclusive)
}

// Quantile gets the approximate quantile of the given normalized rank.
// normRank is the normalized rank in the range [0.0, 1.0].
// If isInclusive is true, rank includes all the quantiles less than or equal to
// the quantile directly corresponding to the given rank.
// If not, rank includes all the quantiles less than
// the quantile directly corresponding to the given rank.
// The default option is inclusive.
func (s *Sketch) Quantile(normRank float64, opts ...SearchCriteriaOptionFunc) (float32, error) {
	if s.IsEmpty() {
		return 0, ErrEmpty
	}

	if err := s.refreshSortedView(); err != nil {
		return 0, err
	}

	options := &searchCriteria{
		isInclusive: true,
	}
	for _, opt := range opts {
		opt(options)
	}

	return s.sortedView.Quantile(normRank, options.isInclusive)
}

// Quantiles gets quantiles from the given array of normalized ranks.
// ranks is the normalized ranks, each of which must be in the valid interval [0.0, 1.0].
// The default option is inclusive search.
func (s *Sketch) Quantiles(ranks []float64, opts ...SearchCriteriaOptionFunc) ([]float32, error) {
	if s.IsEmpty() {
		return nil, ErrEmpty
	}

	if err := s.refreshSortedView(); err != nil {
		return nil, err
	}

	options := &searchCriteria{
		isInclusive: true,
	}
	for _, opt := range opts {
		opt(options)
	}

	length := len(ranks)
	quantiles := make([]float32, 0, length)
	for i := 0; i < length; i++ {
		quantile, err := s.sortedView.Quantile(ranks[i], options.isInclusive)
		if err != nil {
			return nil, err
		}

		quantiles = append(quantiles, quantile)
	}
	return quantiles, nil
}

type confidenceOptions struct {
	numStdDev int
}

// ConfidenceOptionFunc is a function type that configures confidence interval options.
type ConfidenceOptionFunc func(*confidenceOptions)

// WithNumStdDev sets the number of standard deviations to use for confidence intervals.
func WithNumStdDev(n int) ConfidenceOptionFunc {
	return func(opt *confidenceOptions) {
		opt.numStdDev = n
	}
}

// QuantileLowerBound returns the lower bound of the quantile confidence interval
// in which the quantile of the given rank exists.
// numStdDev is the number of standard deviations. Must be 1, 2, or 3. default numStdDev is 2.
// When numStdDev is 2, the approximate probability that the true quantile is within
// the confidence interval specified by the upper and lower quantile bounds
// for this sketch is 0.95.
func (s *Sketch) QuantileLowerBound(
	rank float64, opts ...ConfidenceOptionFunc,
) (float32, error) {
	lb, err := s.RankLowerBound(rank, opts...)
	if err != nil {
		return 0, err
	}
	return s.Quantile(lb)
}

// RankLowerBound returns the approximate lower bound of a rank confidence interval
// which the true rank of the given rank exists.
// rank should be in the 0 to 1.0.
// numStdDev is the number of standard deviations. Must be 1, 2, or 3. default numStdDev is 2.
// When numStdDev is 2, the approximate probability that the true quantile is within
// the confidence interval specified by the upper and lower quantile bounds
// for this sketch is 0.95.
func (s *Sketch) RankLowerBound(
	rank float64, opts ...ConfidenceOptionFunc,
) (float64, error) {
	options := &confidenceOptions{
		numStdDev: 2,
	}
	for _, opt := range opts {
		opt(options)
	}

	return computeRankLowerBound(
		s.k, s.numLevels(), rank, options.numStdDev, s.isHighRankAccuracyMode, s.n,
	), nil
}

// numLevels returns number of levels of compactors in the sketch.
func (s *Sketch) numLevels() int {
	return len(s.compactors)
}

// QuantileUpperBound returns the upper bound of the quantile confidence interval
// in which the quantile of the given rank exists.
// numStdDev is the number of standard deviations. Must be 1, 2, or 3. default numStdDev is 2.
// When numStdDev is 2, the approximate probability that the true quantile is within
// the confidence interval specified by the upper and lower quantile bounds
// for this sketch is 0.95.
func (s *Sketch) QuantileUpperBound(
	rank float64, opts ...ConfidenceOptionFunc,
) (float32, error) {
	ub, err := s.RankUpperBound(rank, opts...)
	if err != nil {
		return 0, err
	}
	return s.Quantile(ub)
}

// RankUpperBound returns the approximate upper bound of the rank confidence interval
// in which the true rank of the given rank exists.
// rank should be in the 0 to 1.0.
// numStdDev is the number of standard deviations. Must be 1, 2, or 3. default numStdDev is 2.
// When numStdDev is 2, the approximate probability that the true quantile is within
// the confidence interval specified by the upper and lower quantile bounds
// for this sketch is 0.95.
func (s *Sketch) RankUpperBound(
	rank float64, opts ...ConfidenceOptionFunc,
) (float64, error) {
	options := &confidenceOptions{
		numStdDev: 2,
	}
	for _, opt := range opts {
		opt(options)
	}

	return computeRankUpperBound(
		s.k, s.numLevels(), rank, options.numStdDev, s.isHighRankAccuracyMode, s.n,
	), nil
}

// Rank returns normalized rank corresponding to the given quantile and search criterion.
// The default option is inclusive search.
func (s *Sketch) Rank(quantile float32, opts ...SearchCriteriaOptionFunc) (float64, error) {
	if s.IsEmpty() {
		return 0, ErrEmpty
	}
	if math.IsNaN(float64(quantile)) {
		return 0, errors.New("quantile must not be NaN")
	}
	if math.IsInf(float64(quantile), 0) || math.IsInf(float64(quantile), -1) {
		return 0, errors.New("quantile must be finite")
	}

	if err := s.refreshSortedView(); err != nil {
		return 0, err
	}

	options := &searchCriteria{
		isInclusive: true,
	}
	for _, opt := range opts {
		opt(options)
	}

	return s.sortedView.Rank(quantile, options.isInclusive)
}

// Ranks returns normalized ranks corresponding to the given quantiles and search criterion.
// The default option is inclusive.
func (s *Sketch) Ranks(quantiles []float32, opts ...SearchCriteriaOptionFunc) ([]float64, error) {
	if s.IsEmpty() {
		return nil, ErrEmpty
	}

	length := len(quantiles)
	ranks := make([]float64, 0, length)
	for i := 0; i < length; i++ {
		rank, err := s.Rank(quantiles[i], opts...)
		if err != nil {
			return nil, err
		}

		ranks = append(ranks, rank)
	}
	return ranks, nil
}

// NumRetained returns the number of quantiles retained by the sketch.
func (s *Sketch) NumRetained() int {
	return s.numRetained
}

// IsEstimationMode returns true if the sketch is in estimation mode.
func (s *Sketch) IsEstimationMode() bool {
	return s.numLevels() > 1
}

// All returns all retained items of the sketch.
func (s *Sketch) All() []Item {
	if s.numRetained == 0 {
		return nil
	}

	var (
		itemIndex        = 0
		compactorIndex   = 0
		items            []Item
		currentCompactor = s.compactors[0]
	)
	for compactorIndex < len(s.compactors) {
		quantile := currentCompactor.Item(itemIndex)
		weight := int64(1) << compactorIndex

		items = append(items, Item{
			Quantile: quantile,
			Weight:   weight,
		})

		if itemIndex == currentCompactor.Count()-1 {
			compactorIndex++
			if compactorIndex >= len(s.compactors) {
				break
			}

			currentCompactor = s.compactors[compactorIndex]
			itemIndex = 0
			continue
		}

		itemIndex++
	}

	return items
}

// Merge merges another sketch into this one. The other sketch is not modified.
func (s *Sketch) Merge(other *Sketch) error {
	if other == nil || other.IsEmpty() {
		return nil
	}

	if s.isHighRankAccuracyMode != other.isHighRankAccuracyMode {
		return errors.New("both sketches must have the same HighRankAccuracy setting")
	}

	s.n += other.n

	if math.IsNaN(float64(s.minItem)) || other.minItem < s.minItem {
		s.minItem = other.minItem
	}
	if math.IsNaN(float64(s.maxItem)) || other.maxItem > s.maxItem {
		s.maxItem = other.maxItem
	}

	// grow until self has at least as many compactors as others.
	for i := s.numLevels(); i < other.numLevels(); i++ {
		s.grow()
	}

	// merge items in all height compactors.
	for i := 0; i < other.numLevels(); i++ {
		if err := s.compactors[i].Merge(other.compactors[i]); err != nil {
			return err
		}
	}
	s.maxNomSize = s.computeMaxNomSize()
	s.numRetained = s.computeRetainedItems()
	if s.numRetained >= s.maxNomSize {
		if err := s.compress(); err != nil {
			return err
		}
	}

	if s.numRetained >= s.maxNomSize {
		return fmt.Errorf(
			"sketch is in invalid state. retained items should be less than max nominal size. retained: %d, max nominal size: %d",
			s.numRetained, s.maxNomSize,
		)
	}

	s.sortedView = nil

	return nil
}

// Reset the sketch to the empty state.
func (s *Sketch) Reset() {
	s.n = 0
	s.numRetained = 0
	s.maxNomSize = 0
	s.minItem = float32(math.NaN())
	s.maxItem = float32(math.NaN())
	s.sortedView = nil
	s.compactors = s.compactors[:0]
	s.grow()
}

// String returns a string representation of the sketch.
func (s *Sketch) String() string {
	var result strings.Builder
	result.WriteString("**********Relative Error Quantiles Sketch Summary**********")
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   K                    : %d", s.k))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   N                    : %d", s.n))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   Retained Items       : %d", s.numRetained))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   Min Item             : %f", s.minItem))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   Max Item             : %f", s.maxItem))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   Estimation Mode      : %v", s.IsEstimationMode()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   High Rank Acc        : %v", s.isHighRankAccuracyMode))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   Levels:              : %d", s.numLevels()))
	result.WriteString("\n")
	result.WriteString("************************End Summary************************")
	result.WriteString("\n")

	return result.String()
}

// Update updates this sketch with the given item.
// NaN are ignored.
func (s *Sketch) Update(item float32) error {
	if math.IsNaN(float64(item)) {
		return nil
	}

	if s.IsEmpty() {
		s.minItem = item
		s.maxItem = item
	} else {
		s.minItem = min(item, s.minItem)
		s.maxItem = max(item, s.maxItem)
	}

	comp := s.compactors[0]
	comp.Append(item)
	s.numRetained++
	s.n++
	if s.numRetained >= s.maxNomSize {
		comp.Sort()
		if err := s.compress(); err != nil {
			return err
		}
	}

	s.sortedView = nil

	return nil
}

// CompactorDetailString returns a string representation of the compactors in the sketch.
// Each compactor string is prepended by the compactor lgWeight,
// the current number of retained quantiles of the compactor and
// the current nominal capacity of the compactor.
func (s *Sketch) CompactorDetailString(showAllData bool) string {
	var result strings.Builder
	result.WriteString("*********Relative Error Quantiles Compactor Detail*********")
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("Compactor Detail: Ret Items: %d  N: %d", s.numRetained, s.n))
	result.WriteString("\n")
	for _, comp := range s.compactors {
		result.WriteString(comp.String())
		result.WriteString("\n")
		if showAllData {
			result.WriteString(comp.itemsToString(20))
			result.WriteString("\n")
		}
	}
	result.WriteString("************************End Detail*************************")
	return result.String()
}

// SortedView returns a sorted view of the data retained by the sketch, or an error if refreshing the view fails.
func (s *Sketch) SortedView() (*quantilecommon.NumericSortedView[float32], error) {
	if err := s.refreshSortedView(); err != nil {
		return nil, err
	}

	return s.sortedView, nil
}

func (s *Sketch) computeRetainedItems() int {
	count := 0
	for _, comp := range s.compactors {
		count += comp.Count()
	}
	return count
}

func (s *Sketch) compress() error {
	for i := 0; i < len(s.compactors); i++ {
		comp := s.compactors[i]
		retainedItemsInCompactor := comp.Count()
		nomCapInCompactor := comp.NomCapacity()

		if retainedItemsInCompactor >= nomCapInCompactor {
			if i+1 >= s.numLevels() { // at the top.
				s.grow() // add a new level, increase maxNomSize.
			}

			result, err := comp.Compact(s.compactors[i+1])
			if err != nil {
				return err
			}

			s.numRetained += result.deltaRetItems
			s.maxNomSize += result.deltaNomSize
		}
	}

	s.sortedView = nil

	return nil
}

func computeRankLowerBound(
	k int,
	levels int,
	rank float64,
	numStdDev int,
	isHighRankAccuracyMode bool,
	n int64,
) float64 {
	if isExactRank(k, levels, rank, isHighRankAccuracyMode, n) {
		return rank
	}

	relative := (relRSEFactor / float64(k)) * rank
	if isHighRankAccuracyMode {
		relative = (relRSEFactor / float64(k)) * (1.0 - rank)
	}
	fixed := fixRSEFactor / float64(k)
	lbRel := rank - (float64(numStdDev) * relative)
	lbFix := rank - (float64(numStdDev) * fixed)
	return max(lbRel, lbFix)
}

func isExactRank(
	k int, levels int, rank float64, isHighRankAccuracyMode bool, n int64,
) bool {
	baseCap := k * initNumberOfSections
	if levels == 1 || n <= int64(baseCap) {
		return true
	}
	exactRankThreshold := float64(baseCap) / float64(n)
	if isHighRankAccuracyMode {
		return rank >= (1.0 - exactRankThreshold)
	}
	return rank <= exactRankThreshold
}

func computeRankUpperBound(
	k int,
	levels int,
	rank float64,
	numStdDev int,
	isHighRankAccuracyMode bool,
	n int64,
) float64 {
	if isExactRank(k, levels, rank, isHighRankAccuracyMode, n) {
		return rank
	}

	relative := (relRSEFactor / float64(k)) * rank
	if isHighRankAccuracyMode {
		relative = (relRSEFactor / float64(k)) * (1.0 - rank)
	}
	fixed := fixRSEFactor / float64(k)
	lbRel := rank + (float64(numStdDev) * relative)
	lbFix := rank + (float64(numStdDev) * fixed)
	return min(lbRel, lbFix)
}

// Item represents a quantile value and its associated weight retained by the sketch.
type Item struct {
	Quantile float32
	Weight   int64
}
