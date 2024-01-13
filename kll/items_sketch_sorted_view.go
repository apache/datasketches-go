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
	"errors"
	"github.com/apache/datasketches-go/internal"
	"sort"
)

type itemsSketchSortedView[C comparable] struct {
	quantiles     []C
	cumWeights    []int64
	totalN        uint64
	maxItem       C
	minItem       C
	itemsSketchOp ItemSketchOp[C]
}

func newItemsSketchSortedView[C comparable](sketch *ItemsSketch[C]) (*itemsSketchSortedView[C], error) {
	if sketch.IsEmpty() {
		return nil, errors.New("empty sketch")
	}
	totalN := sketch.GetN()
	srcQuantiles := sketch.GetTotalItemsArray()
	srcLevels := sketch.levels
	srcNumLevels := sketch.numLevels
	maxItem, err := sketch.GetMaxItem()
	if err != nil {
		return nil, err
	}
	minItem, err := sketch.GetMinItem()
	if err != nil {
		return nil, err
	}

	if totalN == 0 {
		return nil, errors.New("empty sketch")
	}
	if !sketch.IsLevelZeroSorted() {
		subSlice := srcQuantiles[srcLevels[0]:srcLevels[1]]
		lessFn := sketch.itemsSketchOp.lessFn()
		sort.Slice(subSlice, func(a, b int) bool {
			return lessFn(subSlice[a], subSlice[b])
		})
	}
	numQuantiles := srcLevels[srcNumLevels] - srcLevels[0]

	quantiles, cumWeights := populateFromSketch(srcQuantiles, srcLevels, srcNumLevels, numQuantiles, sketch.itemsSketchOp)
	return &itemsSketchSortedView[C]{
		quantiles:     quantiles,
		cumWeights:    cumWeights,
		totalN:        totalN,
		maxItem:       maxItem,
		minItem:       minItem,
		itemsSketchOp: sketch.itemsSketchOp,
	}, nil
}

func (s *itemsSketchSortedView[C]) GetRank(item C, inclusive bool) (float64, error) {
	if s.totalN == 0 {
		return 0, errors.New("empty sketch")
	}
	length := len(s.quantiles)
	crit := internal.InequalityLT
	if inclusive {
		crit = internal.InequalityLE
	}
	index := internal.FindWithInequality(s.quantiles, 0, length-1, item, crit, s.itemsSketchOp.lessFn())
	if index == -1 {
		return 0, nil //EXCLUSIVE (LT) case: quantile <= minQuantile; INCLUSIVE (LE) case: quantile < minQuantile
	}
	return float64(s.cumWeights[index]) / float64(s.totalN), nil
}

func (s *itemsSketchSortedView[C]) GetQuantile(rank float64, inclusive bool) (C, error) {
	if s.totalN == 0 {
		return s.itemsSketchOp.identity(), errors.New("empty sketch")
	}
	err := checkNormalizedRankBounds(rank)
	if err != nil {
		return s.itemsSketchOp.identity(), err
	}
	index := s.getQuantileIndex(rank, inclusive)
	return s.quantiles[index], nil
	/*
	   QuantilesUtil.checkNormalizedRankBounds(rank);
	   final int index = getQuantileIndex(rank, searchCrit);
	   return quantiles[index];
	*/
}

func (s *itemsSketchSortedView[C]) GetPMF(splitPoints []C, size uint32, inclusive bool) ([]float64, error) {
	if s.totalN == 0 {
		return nil, errors.New("empty sketch")
	}
	panic("TODO itemsSketchSortedView GetPMF")
}

/*
template<typename T, typename C, typename A>
auto quantiles_sorted_view<T, C, A>::get_PMF(const T* split_points, uint32_t size, bool inclusive) const -> vector_double {
  auto buckets = get_CDF(split_points, size, inclusive);
  if (buckets.size() == 0) return buckets;
  for (uint32_t i = size; i > 0; --i) {
    buckets[i] -= buckets[i - 1];
  }
  return buckets;
}

*/

func (s *itemsSketchSortedView[C]) GetCDF(splitPoints []C, size uint32, inclusive bool) ([]float64, error) {
	if s.totalN == 0 {
		return nil, errors.New("empty sketch")
	}
	panic("TODO itemsSketchSortedView GetCDF")
}

/*
  public double[] getCDF(final T[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    GenericSortedView.validateItems(splitPoints, comparator);
    final int len = splitPoints.length + 1;
    final double[] buckets = new double[len];
    for (int i = 0; i < len - 1; i++) {
      buckets[i] = getRank(splitPoints[i], searchCrit);
    }
    buckets[len - 1] = 1.0;
    return buckets;
  }
*/

func (s *itemsSketchSortedView[C]) getQuantileIndex(rank float64, inclusive bool) int {
	length := len(s.quantiles)
	naturalRank := getNaturalRank(rank, s.totalN, inclusive)
	crit := internal.InequalityGE
	if inclusive {
		crit = internal.InequalityGT
	}
	index := internal.FindWithInequality(s.cumWeights, 0, length-1, naturalRank, crit, func(a, b int64) bool {
		return a < b
	})
	if index == -1 {
		return length - 1
	}
	return index
}

func populateFromSketch[C comparable](srcQuantiles []C, levels []uint32, numLevels uint8, numQuantiles uint32, itemsSketchOp ItemSketchOp[C]) ([]C, []int64) {
	quantiles := make([]C, numQuantiles)
	cumWeights := make([]int64, numQuantiles)
	myLevels := make([]uint32, numLevels+1)
	offset := levels[0]
	for i := uint32(0); i < numQuantiles; i++ {
		quantiles[i] = srcQuantiles[i+offset]
	}
	srcLevel := uint8(0)
	dstLevel := uint8(0)
	weight := int64(1)
	for srcLevel < numLevels {
		fromIndex := levels[srcLevel] - offset
		toIndex := levels[srcLevel+1] - offset // exclusive
		if fromIndex < toIndex {               // if equal, skip empty level
			for i := fromIndex; i < toIndex; i++ {
				cumWeights[i] = weight
			}
			myLevels[dstLevel] = fromIndex
			myLevels[dstLevel+1] = toIndex
			dstLevel++
		}
		srcLevel++
		weight *= 2
	}
	numLevels = dstLevel
	blockyTandemMergeSort(quantiles, cumWeights, myLevels, numLevels, itemsSketchOp) //create unit weights
	convertToCumulative(cumWeights)
	return quantiles, cumWeights
}

func blockyTandemMergeSort[C comparable](quantiles []C, weights []int64, levels []uint32, numLevels uint8, itemsSketchOp ItemSketchOp[C]) {
	if numLevels == 1 {
		return
	}

	// duplicate the input in preparation for the "ping-pong" copy reduction strategy.
	quantilesTmp := make([]C, len(quantiles))
	copy(quantiles, quantilesTmp)
	weightsTmp := make([]int64, len(weights))
	copy(weights, weightsTmp) // don't need the extra one here

	blockyTandemMergeSortRecursion(quantilesTmp, weightsTmp, quantiles, weights, levels, 0, numLevels, itemsSketchOp)
}

func blockyTandemMergeSortRecursion[C comparable](quantilesSrc []C, weightsSrc []int64, quantilesDst []C, weightsDst []int64, levels []uint32, startingLevel uint8, numLevels uint8, itemsSketchOp ItemSketchOp[C]) {
	if numLevels == 1 {
		return
	}
	numLevels1 := numLevels / 2
	numLevels2 := numLevels - numLevels1
	startingLevel1 := startingLevel
	startingLevel2 := startingLevel + numLevels1
	// swap roles of src and dst
	blockyTandemMergeSortRecursion(quantilesDst, weightsDst, quantilesSrc, weightsSrc, levels, startingLevel1, numLevels1, itemsSketchOp)
	blockyTandemMergeSortRecursion(quantilesDst, weightsDst, quantilesSrc, weightsSrc, levels, startingLevel2, numLevels2, itemsSketchOp)
	tandemMerge(quantilesSrc, weightsSrc, quantilesDst, weightsDst, levels, startingLevel1, numLevels1, startingLevel2, numLevels2, itemsSketchOp)
}

func tandemMerge[C comparable](quantilesSrc []C, weightsSrc []int64, quantilesDst []C, weightsDst []int64, levels []uint32, startingLevel1 uint8, numLevels1 uint8, startingLevel2 uint8, numLevels2 uint8, itemsSketchOp ItemSketchOp[C]) {
	fromIndex1 := levels[startingLevel1]
	toIndex1 := levels[startingLevel1+numLevels1] // exclusive
	fromIndex2 := levels[startingLevel2]
	toIndex2 := levels[startingLevel2+numLevels2] // exclusive
	iSrc1 := fromIndex1
	iSrc2 := fromIndex2
	iDst := fromIndex1

	lessFn := itemsSketchOp.lessFn()
	for iSrc1 < toIndex1 && iSrc2 < toIndex2 {
		if lessFn(quantilesSrc[iSrc1], quantilesSrc[iSrc2]) {
			quantilesDst[iDst] = quantilesSrc[iSrc1]
			weightsDst[iDst] = weightsSrc[iSrc1]
			iSrc1++
		} else {
			quantilesDst[iDst] = quantilesSrc[iSrc2]
			weightsDst[iDst] = weightsSrc[iSrc2]
			iSrc2++
		}
		iDst++
	}
	if iSrc1 < toIndex1 {
		copy(quantilesSrc[iSrc1:toIndex1], quantilesDst[iDst:toIndex1])
		copy(weightsSrc[iSrc1:toIndex1], weightsDst[iDst:toIndex1])
	} else if iSrc2 < toIndex2 {
		copy(quantilesSrc[iSrc2:toIndex2], quantilesDst[iDst:toIndex2])
		copy(weightsSrc[iSrc2:toIndex2], weightsDst[iDst:toIndex2])
	}
}
