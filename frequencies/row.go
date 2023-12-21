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
	"fmt"
	"sort"
)

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

func (r *Row) getEstimate() int64 {
	return r.est
}

func (r *Row) getUpperBound() int64 {
	return r.ub
}

func (r *Row) getLowerBound() int64 {
	return r.lb
}

func sortItems(sk *LongsSketch, threshold int64, errorType ErrorType) ([]*Row, error) {
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
