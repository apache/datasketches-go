package kll

type ItemsSketchSortedViewIterator[C comparable] struct {
	quantiles  []C
	cumWeights []int64
	totalN     int64
	index      int
}

func newItemsSketchSortedViewIterator[C comparable](quantiles []C, cumWeights []int64) *ItemsSketchSortedViewIterator[C] {
	totalN := int64(0)
	if len(cumWeights) > 0 {
		totalN = cumWeights[len(cumWeights)-1]
	}
	return &ItemsSketchSortedViewIterator[C]{
		quantiles:  quantiles,
		cumWeights: cumWeights,
		totalN:     totalN,
		index:      -1,
	}
}

func (i *ItemsSketchSortedViewIterator[C]) Next() bool {
	i.index++
	return i.index < len(i.cumWeights)
}

func (i *ItemsSketchSortedViewIterator[C]) GetQuantile() C {
	return i.quantiles[i.index]
}

func (i *ItemsSketchSortedViewIterator[C]) GetWeight() int64 {
	if i.index == 0 {
		return i.cumWeights[0]
	}
	return i.cumWeights[i.index] - i.cumWeights[i.index-1]
}

func (i *ItemsSketchSortedViewIterator[C]) GetNaturalRank(inclusive bool) int64 {
	if inclusive {
		return i.cumWeights[i.index]
	}
	if i.index == 0 {
		return 0
	}
	return i.cumWeights[i.index-1]
}

func (i *ItemsSketchSortedViewIterator[C]) GetNormalizedRank(inclusive bool) float64 {
	return float64(i.GetNaturalRank(inclusive)) / float64(i.totalN)
}
