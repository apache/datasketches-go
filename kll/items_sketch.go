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

package kll

import (
	"fmt"
)

type lessFn[C comparable] func(int, int) bool

type ItemSketchOp[C comparable] interface {
	identity() C
	lessFn(list []C) lessFn[C]
}

type ItemsSketch[C comparable] struct {
	k                 uint16
	m                 uint8
	minK              uint16
	numLevels         uint8
	isLevelZeroSorted bool
	n                 uint64
	levels            []uint32
	items             []C
	itemsSize         uint32
	minItem           *C
	maxItem           *C
	sortedView        *itemsSketchSortedView[C]
	itemsSketchOp     ItemSketchOp[C]
}

const (
	_DEFAULT_K = uint16(200)
	_DEFAULT_M = uint8(8)
	_MIN_K     = uint16(_DEFAULT_M)
	_MAX_K     = (1 << 16) - 1
)

func NewItemsSketch[C comparable](k uint16, itemSketchOp ItemSketchOp[C]) (*ItemsSketch[C], error) {
	if k < _MIN_K || k > _MAX_K {
		return nil, fmt.Errorf("k must be >= %d and <= %d: %d", _MIN_K, _MAX_K, k)
	}
	return &ItemsSketch[C]{
		k,
		_DEFAULT_M,
		k,
		uint8(1),
		false,
		0,
		[]uint32{uint32(k), uint32(k)},
		make([]C, k),
		uint32(k),
		nil,
		nil,
		nil,
		itemSketchOp,
	}, nil
}

func (s *ItemsSketch[C]) IsEmpty() bool {
	return s.n == 0
}

func (s *ItemsSketch[C]) GetN() uint64 {
	return s.n
}

func (s *ItemsSketch[C]) GetNumRetained() uint32 {
	return s.levels[s.numLevels] - s.levels[0]
}

func (s *ItemsSketch[C]) GetMinItem() (C, error) {
	if s.IsEmpty() {
		return s.itemsSketchOp.identity(), fmt.Errorf("operation is undefined for an empty sketch")
	}
	return *s.minItem, nil
}

func (s *ItemsSketch[C]) GetMaxItem() (C, error) {
	if s.IsEmpty() {
		return s.itemsSketchOp.identity(), fmt.Errorf("operation is undefined for an empty sketch")
	}
	return *s.maxItem, nil
}

func (s *ItemsSketch[C]) IsEstimationMode() bool {
	return s.numLevels > 1
}

func (s *ItemsSketch[C]) IsLevelZeroSorted() bool {
	return s.isLevelZeroSorted
}

func (s *ItemsSketch[C]) GetTotalItemsArray() []C {
	if s.n == 0 {
		return make([]C, s.k)
	}
	outArr := make([]C, len(s.items))
	copy(outArr, s.items)
	return outArr
}

func (s *ItemsSketch[C]) GetRank(item C, inclusive bool) (float64, error) {
	if s.IsEmpty() {
		return 0, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return 0, err
	}
	return s.sortedView.GetRank(item, inclusive)
}

/*
template<typename T, typename C, typename A>
auto kll_sketch<T, C, A>::get_quantile(double rank, bool inclusive) const -> quantile_return_type {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  if ((rank < 0.0) || (rank > 1.0)) {
    throw std::invalid_argument("normalized rank cannot be less than zero or greater than 1.0");
  }
  // may have a side effect of sorting level zero if needed
  setup_sorted_view();
  return sorted_view_->get_quantile(rank, inclusive);
}
*/

func (s *ItemsSketch[C]) GetQuantile(rank float64, inclusive bool) (C, error) {
	if s.IsEmpty() {
		return s.itemsSketchOp.identity(), fmt.Errorf("operation is undefined for an empty sketch")
	}
	if rank < 0.0 || rank > 1.0 {
		return s.itemsSketchOp.identity(), fmt.Errorf("normalized rank cannot be less than zero or greater than 1.0: %f", rank)
	}
	err := s.setupSortedView()
	if err != nil {
		return s.itemsSketchOp.identity(), err
	}
	return s.sortedView.GetQuantile(rank, inclusive)
}

func (s *ItemsSketch[C]) GetPMF(splitPoints []C, size uint32, inclusive bool) ([]float64, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	return s.sortedView.GetPMF(splitPoints, size, inclusive)
}

func (s *ItemsSketch[C]) setupSortedView() error {
	if s.sortedView == nil {
		sView, err := newItemsSketchSortedView[C](s)
		if err != nil {
			return err
		}
		s.sortedView = sView
	}
	return nil
}
