package frequencies

import (
	"fmt"
	"sort"
)

/*
  public static class Row implements Comparable<Row> {
    final long item;
    final long est;
    final long ub;
    final long lb;
    private static final String fmt =  ("  %20d%20d%20d %d");
    private static final String hfmt = ("  %20s%20s%20s %s");

    Row(final long item, final long estimate, final long ub, final long lb) {
      this.item = item;
      est = estimate;
      this.ub = ub;
      this.lb = lb;
    }
*/

const (
	hfmt string = "  %20s%20s%20s %s"
)

type Row struct {
	item int64
	est  int64
	ub   int64
	lb   int64
}

func NewRow(item int64, estimate int64, ub int64, lb int64) Row {
	return Row{
		item: item,
		est:  estimate,
		ub:   ub,
		lb:   lb,
	}
}

func (r *Row) String() string {
	return fmt.Sprintf("  %20d%20d%20d %d", r.item, r.est, r.ub, r.lb)
}

func sortItems(sk *LongSketch, threshold int64, errorType ErrorType) ([]*Row, error) {
	rowList := make([]*Row, 0)
	iter := sk.hashMap.iterator()
	if errorType == NO_FALSE_NEGATIVES {
		for iter.next() {
			est, err := sk.getEstimate(iter.getKey())
			if err != nil {
				return nil, err
			}
			ub, err := sk.getUpperBound(iter.getKey())
			if err != nil {
				return nil, err
			}
			lb, err := sk.getLowerBound(iter.getKey())
			if err != nil {
				return nil, err
			}
			if ub >= threshold {
				row := NewRow(iter.getKey(), est, ub, lb)
				rowList = append(rowList, &row)
			}
		}
	} else { //NO_FALSE_POSITIVES
		for iter.next() {
			est, err := sk.getEstimate(iter.getKey())
			if err != nil {
				return nil, err
			}
			ub, err := sk.getUpperBound(iter.getKey())
			if err != nil {
				return nil, err
			}
			lb, err := sk.getLowerBound(iter.getKey())
			if err != nil {
				return nil, err
			}
			if lb >= threshold {
				row := NewRow(iter.getKey(), est, ub, lb)
				rowList = append(rowList, &row)
			}
		}
	}

	sort.Slice(rowList, func(i, j int) bool {
		return rowList[i].est < rowList[j].est
	})

	return rowList, nil
}
