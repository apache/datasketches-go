package kll

import "errors"

type ItemsPartitionBoundaries[C comparable] struct {
	totalN     uint64    //totalN of source sketch
	boundaries []C       //quantiles at the boundaries
	natRanks   []int64   //natural ranks at the boundaries
	normRanks  []float64 //normalized ranks at the boundaries
	maxItem    C         //of the source sketch
	minItem    C         //of the source sketch
	inclusive  bool      //of the source sketch query to getPartitionBoundaries.
	//computed
	numDeltaItems []int64 //num of items in each part
	numPartitions int     //num of partitions
}

func newItemsPartitionBoundaries[C comparable](totalN uint64, boundaries []C, natRanks []int64, normRanks []float64, maxItem C, minItem C, inclusive bool) (*ItemsPartitionBoundaries[C], error) {
	if len(boundaries) < 2 {
		return nil, errors.New("boundaries must have at least 2 items")
	}
	numDeltaItems := make([]int64, len(boundaries))
	numDeltaItems[0] = 0
	for i := 1; i < len(boundaries); i++ {
		addOne := 0
		if (i == 1 && inclusive) || (i == len(boundaries)-1 && !inclusive) {
			addOne = 1
		}
		numDeltaItems[i] = natRanks[i] - natRanks[i-1] + int64(addOne)
	}
	return &ItemsPartitionBoundaries[C]{
		totalN:        totalN,
		boundaries:    boundaries,
		natRanks:      natRanks,
		normRanks:     normRanks,
		maxItem:       maxItem,
		minItem:       minItem,
		inclusive:     inclusive,
		numDeltaItems: numDeltaItems,
		numPartitions: len(boundaries) - 1,
	}, nil
}

func (b *ItemsPartitionBoundaries[C]) GetBoundaries() []C {
	return b.boundaries
}
