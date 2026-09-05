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

package frequencies

import (
	"math/bits"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/datasketches-go/common"
)

// Sinks, to keep the compiler from eliding the benchmarked work.
var (
	benchRowPointerSink     []*Row
	benchRowValueSink       []Row
	benchRowItemPointerSink []*RowItem[string]
	benchRowItemValueSink   []RowItem[string]
)

func benchMapSizeFor(n int) int {
	need := (n*4 + 2) / 3
	if need < 1<<_LG_MIN_MAP_SIZE {
		return 1 << _LG_MIN_MAP_SIZE
	}
	return 1 << bits.Len(uint(need-1))
}

func newBenchLongsSketch(b *testing.B, n int) *LongsSketch {
	b.Helper()
	sk, err := NewLongsSketchWithMaxMapSize(benchMapSizeFor(n))
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := sk.UpdateMany(int64(i), int64(i%97)+1); err != nil {
			b.Fatal(err)
		}
	}
	if got := sk.GetNumActiveItems(); got != n {
		b.Fatalf("want %d active items, got %d (sketch purged)", n, got)
	}
	return sk
}

func newBenchItemsSketch(b *testing.B, n int) *ItemsSketch[string] {
	b.Helper()
	sk, err := NewFrequencyItemsSketchWithMaxMapSize[string](
		benchMapSizeFor(n), common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := sk.UpdateMany("item"+strconv.Itoa(i), int64(i%97)+1); err != nil {
			b.Fatal(err)
		}
	}
	if got := sk.GetNumActiveItems(); got != n {
		b.Fatalf("want %d active items, got %d (sketch purged)", n, got)
	}
	return sk
}

func keep(errorType errorType, lb, ub, threshold int64) bool {
	if errorType == ErrorTypeEnum.NoFalseNegatives {
		return ub >= threshold
	}
	return lb >= threshold
}

// sortItemsPointerLong is the pre-change LongsSketch.sortItems, kept for comparison.
func sortItemsPointerLong(s *LongsSketch, threshold int64, errorType errorType) ([]*Row, error) {
	rowList := make([]*Row, 0)
	iter := s.hashMap.iterator()
	for iter.next() {
		est, lb, ub, err := s.frequencies(iter.getKey())
		if err != nil {
			return nil, err
		}
		if keep(errorType, lb, ub, threshold) {
			rowList = append(rowList, &Row{item: iter.getKey(), est: est, ub: ub, lb: lb})
		}
	}
	slices.SortFunc(rowList, func(a, b *Row) int {
		if a.est > b.est {
			return -1
		}
		if a.est < b.est {
			return 1
		}
		return 0
	})
	return rowList, nil
}

// sortItemsPointerItem is the pre-change ItemsSketch.sortItems, kept for comparison.
func sortItemsPointerItem[C comparable](i *ItemsSketch[C], threshold int64, errorType errorType) ([]*RowItem[C], error) {
	rowList := make([]*RowItem[C], 0)
	iter := i.hashMap.iterator()
	for iter.next() {
		est, lb, ub, err := i.frequencies(iter.getKey())
		if err != nil {
			return nil, err
		}
		if keep(errorType, lb, ub, threshold) {
			rowList = append(rowList, &RowItem[C]{item: iter.getKey(), est: est, ub: ub, lb: lb})
		}
	}
	slices.SortFunc(rowList, func(a, b *RowItem[C]) int {
		if a.est > b.est {
			return -1
		}
		if a.est < b.est {
			return 1
		}
		return 0
	})
	return rowList, nil
}

var benchRowSizes = []int{10, 100, 1000, 10000}

func benchLongs(b *testing.B, run func(*LongsSketch, int64) error) {
	for _, size := range benchRowSizes {
		b.Run("size="+strconv.Itoa(size), func(b *testing.B) {
			sk := newBenchLongsSketch(b, size)
			threshold := sk.GetMaximumError()
			b.ReportAllocs()
			for b.Loop() {
				if err := run(sk, threshold); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchItems(b *testing.B, run func(*ItemsSketch[string], int64) error) {
	for _, size := range benchRowSizes {
		b.Run("size="+strconv.Itoa(size), func(b *testing.B) {
			sk := newBenchItemsSketch(b, size)
			threshold := sk.GetMaximumError()
			b.ReportAllocs()
			for b.Loop() {
				if err := run(sk, threshold); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSortItems_Row_Pointer(b *testing.B) {
	benchLongs(b, func(sk *LongsSketch, th int64) error {
		rows, err := sortItemsPointerLong(sk, th, ErrorTypeEnum.NoFalseNegatives)
		benchRowPointerSink = rows
		return err
	})
}

func BenchmarkSortItems_Row_Value(b *testing.B) {
	benchLongs(b, func(sk *LongsSketch, th int64) error {
		rows, err := sk.sortItems(th, ErrorTypeEnum.NoFalseNegatives)
		benchRowValueSink = rows
		return err
	})
}

func BenchmarkSortItems_RowItem_Pointer(b *testing.B) {
	benchItems(b, func(sk *ItemsSketch[string], th int64) error {
		rows, err := sortItemsPointerItem(sk, th, ErrorTypeEnum.NoFalseNegatives)
		benchRowItemPointerSink = rows
		return err
	})
}

func BenchmarkSortItems_RowItem_Value(b *testing.B) {
	benchItems(b, func(sk *ItemsSketch[string], th int64) error {
		rows, err := sk.sortItems(th, ErrorTypeEnum.NoFalseNegatives)
		benchRowItemValueSink = rows
		return err
	})
}
