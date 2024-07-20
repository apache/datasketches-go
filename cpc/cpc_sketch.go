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

package cpc

import (
	"encoding/binary"
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"github.com/twmb/murmur3"
	"math"
	"math/bits"
	"unsafe"
)

const (
	minLgK = 4
	maxLgK = 26
)

type CpcSketch struct {
	seed uint64

	//common variables
	lgK        int
	numCoupons uint64 // The number of coupons collected so far.
	mergeFlag  bool   // Is the sketch the result of merging?
	fiCol      int    // First Interesting Column. This is part of a speed optimization.

	windowOffset  int
	slidingWindow []byte     //either null or size K bytes
	pairTable     *pairTable //for sparse and surprising values, either null or variable size

	//The following variables are only valid in HIP varients
	kxp         float64 //used with HIP
	hipEstAccum float64 //used with HIP

	scratch [8]byte
}

func NewCpcSketch(lgK int, seed uint64) (*CpcSketch, error) {
	if err := checkLgK(lgK); err != nil {
		return nil, err
	}

	return &CpcSketch{
		lgK:  lgK,
		seed: seed,
		kxp:  float64(int64(1) << lgK),
	}, nil
}

func (c *CpcSketch) GetEstimate() float64 {
	if c.mergeFlag {
		return iconEstimate(c.lgK, c.numCoupons)
	}
	return c.hipEstAccum
}

func (c *CpcSketch) GetLowerBound(kappa int) float64 {
	if c.mergeFlag {
		return iconConfidenceLB(c.lgK, c.numCoupons, kappa)
	}
	return hipConfidenceLB(c.lgK, c.numCoupons, c.hipEstAccum, kappa)
}

func (c *CpcSketch) GetUpperBound(kappa int) float64 {
	if c.mergeFlag {
		return iconConfidenceUB(c.lgK, c.numCoupons, kappa)
	}
	return hipConfidenceUB(c.lgK, c.numCoupons, c.hipEstAccum, kappa)
}

func (c *CpcSketch) UpdateUint64(datum uint64) error {
	binary.LittleEndian.PutUint64(c.scratch[:], datum)
	hashLo, hashHi := hash(c.scratch[:], c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt64(datum int64) error {
	return c.UpdateUint64(uint64(datum))
}

func (c *CpcSketch) UpdateFloat64(datum float64) error {
	binary.LittleEndian.PutUint64(c.scratch[:], math.Float64bits(datum))
	hashLo, hashHi := hash(c.scratch[:], c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateByteSlice(datum []byte) error {
	hashLo, hashHi := hash(datum, c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt64Slice(datum []int64) error {
	hashLo, hashHi := internal.HashInt64SliceMurmur3(datum, 0, len(datum), c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt32Slice(datum []int32) error {
	hashLo, hashHi := internal.HashInt32SliceMurmur3(datum, 0, len(datum), c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateCharSlice(datum []byte) error {
	hashLo, hashHi := internal.HashCharSliceMurmur3(datum, 0, len(datum), c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateString(datum string) error {
	// get a slice to the string data (avoiding a copy to heap)
	return c.UpdateByteSlice(unsafe.Slice(unsafe.StringData(datum), len(datum)))
}

func (c *CpcSketch) hashUpdate(hash0, hash1 uint64) error {
	col := bits.LeadingZeros64(hash1)
	if col < c.fiCol {
		return nil // important speed optimization
	}
	if col > 63 {
		col = 63 // clip so that 0 <= col <= 63
	}
	if c.numCoupons == 0 {
		err := c.promoteEmptyToSparse()
		if err != nil {
			return err
		}
	}
	k := uint64(1) << c.lgK
	row := int(hash0 & (k - 1))
	rowCol := (row << 6) | col

	// Avoid the hash table's "empty" value which is (2^26 -1, 63) (all ones) by changing it
	// to the pair (2^26 - 2, 63), which effectively merges the two cells.
	// This case is *extremely* unlikely, but we might as well handle it.
	// It can't happen at all if lgK (or maxLgK) < 26.
	if rowCol == -1 {
		rowCol ^= 1 << 6 //set the LSB of row to 0
	}

	if (c.numCoupons << 5) < (uint64(3) * k) {
		return c.updateSparse(rowCol)
	} else {
		// TODO(pierre)
		// return c.updateWindowed(rowCol)
	}
	return nil
}

func (c *CpcSketch) promoteEmptyToSparse() error {
	pairTable, err := NewPairTable(2, 6+c.lgK)
	if err != nil {
		return err
	}
	c.pairTable = pairTable
	return nil
}

func (c *CpcSketch) updateSparse(rowCol int) error {
	k := uint64(1) << c.lgK
	c32pre := c.numCoupons << 5
	if c32pre >= (3 * k) {
		// C >= 3K/32, in other words, flavor == SPARSE
		return fmt.Errorf("C >= 3K/32")
	}
	if c.pairTable == nil {
		return fmt.Errorf("pairTable is nil")
	}
	isNovel, err := c.pairTable.maybeInsert(rowCol)
	if err != nil {
		return err
	}
	if isNovel {
		c.numCoupons++
		c.updateHIP(rowCol)
		c32post := c.numCoupons << 5
		if c32post >= (3 * k) {
			c.promoteSparseToWindowed() // C >= 3K/32
		}
	}
	return nil
}

func hash(bs []byte, seed uint64) (uint64, uint64) {
	return murmur3.SeedSum128(seed, seed, bs)
}

func (c *CpcSketch) getFormat() CpcFormat {
	ordinal := 0
	f := c.GetFlavor()
	if f == CpcFlavorHybrid || f == CpcFlavorSparse {
		ordinal = 2
		if !c.mergeFlag {
			ordinal |= 1
		}
	} else {
		ordinal = 0
		if c.slidingWindow != nil {
			ordinal |= 4
		}
		if c.pairTable != nil && c.pairTable.numPairs > 0 {
			ordinal |= 2
		}
		if !c.mergeFlag {
			ordinal |= 1
		}
	}
	return CpcFormat(ordinal)
}

func (c *CpcSketch) GetFlavor() CpcFlavor {
	return determineFlavor(c.lgK, c.numCoupons)
}

func (c *CpcSketch) updateHIP(rowCol int) {
	k := 1 << c.lgK
	col := rowCol & 63
	oneOverP := float64(k) / c.kxp
	c.hipEstAccum += oneOverP
	kxp, _ := internal.InvPow2(col + 1)
	c.kxp -= kxp
}

func (c *CpcSketch) promoteSparseToWindowed() {
	window := make([]byte, 1<<c.lgK)
	newTable, _ := NewPairTable(2, 6+c.lgK)
	oldTable := c.pairTable

	oldSlots := oldTable.slotsArr
	oldNumSlots := 1 << oldTable.lgSizeInts

	for i := 0; i < oldNumSlots; i++ {
		rowCol := oldSlots[i]
		if rowCol != -1 {
			col := rowCol & 63
			if col < 8 {
				row := rowCol >> 6
				window[row] |= 1 << col
			} else {
				newTable.mustInsert(rowCol)
			}
		}
	}

	c.slidingWindow = window
	c.pairTable = newTable
}

/*
  //In terms of flavor, this promotes SPARSE to HYBRID.
  private static void promoteSparseToWindowed(final CpcSketch sketch) {
    final int lgK = sketch.lgK;
    final int k = (1 << lgK);
    final long c32 = sketch.numCoupons << 5;
    assert ((c32 == (3 * k)) || ((lgK == 4) && (c32 > (3 * k))));

    final byte[] window = new byte[k];

    final PairTable newTable = new PairTable(2, 6 + lgK);
    final PairTable oldTable = sketch.pairTable;

    final int[] oldSlots = oldTable.getSlotsArr();
    final int oldNumSlots = (1 << oldTable.getLgSizeInts());

    assert (sketch.windowOffset == 0);

    for (int i = 0; i < oldNumSlots; i++) {
      final int rowCol = oldSlots[i];
      if (rowCol != -1) {
        final int col = rowCol & 63;
        if (col < 8) {
          final int  row = rowCol >>> 6;
          window[row] |= (1 << col);
        }
        else {
          // cannot use Table.mustInsert(), because it doesn't provide for growth
          final boolean isNovel = PairTable.maybeInsert(newTable, rowCol);
          assert (isNovel == true);
        }
      }
    }

    assert (sketch.slidingWindow == null);
    sketch.slidingWindow = window;
    sketch.pairTable = newTable;
  }
*/

func (c *CpcSketch) reset() {
	c.numCoupons = 0
	c.mergeFlag = false
	c.fiCol = 0
	c.windowOffset = 0
	c.slidingWindow = nil
	c.pairTable = nil
	c.kxp = float64(int64(1) << c.lgK)
	c.hipEstAccum = 0
}
