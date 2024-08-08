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
	"fmt"
	"github.com/apache/datasketches-go/internal"
)

type CpcUnion struct {
	seed uint64
	lgK  int

	// Note: at most one of bitMatrix and accumulator will be non-null at any given moment.
	// accumulator is a sketch object that is employed until it graduates out of Sparse mode.
	// At that point, it is converted into a full-sized bitMatrix, which is mathematically a sketch,
	// but doesn't maintain any of the "extra" fields of our sketch objects, so some additional work
	// is required when getResult is called at the end.
	bitMatrix   []uint64
	accumulator CpcSketch
}

func NewCpcUnionSketch(lgK int, seed uint64) (CpcUnion, error) {
	acc, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	if err != nil {
		return CpcUnion{}, err
	}
	return CpcUnion{
		seed: seed,
		lgK:  lgK,
		// We begin with the accumulator holding an EMPTY_MERGED sketch object.
		// As an optimization the accumulator could start as NULL, but that would require changes elsewhere.
		accumulator: acc,
	}, nil
}

func NewCpcUnionSketchWithDefault(lgK int) (CpcUnion, error) {
	return NewCpcUnionSketch(lgK, internal.DEFAULT_UPDATE_SEED)
}

func (u *CpcUnion) Update(source CpcSketch) error {
	if err := checkSeeds(u.seed, source.seed); err != nil {
		return err
	}

	sourceFlavorOrd := source.GetFlavor()
	if sourceFlavorOrd == CpcFlavorEmpty {
		return nil
	}

	// Accumulator and bitMatrix must be mutually exclusive,
	// so bitMatrix != nil => accumulator == nil and visa versa
	// if (Accumulator != nil) union must be EMPTY or SPARSE,
	if err := u.checkUnionState(); err != nil {
		return err
	}

	if source.lgK < u.lgK {
		if err := u.reduceUnionK(source.lgK); err != nil {
			return err
		}
	}

	/*
	   	    if (source == null) { return; }
	          checkSeeds(union.seed, source.seed);

	          final int sourceFlavorOrd = source.getFlavor().ordinal();
	          if (sourceFlavorOrd == 0) { return; } //EMPTY

	          //Accumulator and bitMatrix must be mutually exclusive,
	          //so bitMatrix != null => accumulator == null and visa versa
	          //if (Accumulator != null) union must be EMPTY or SPARSE,
	          checkUnionState(union);

	          if (source.lgK < union.lgK) { reduceUnionK(union, source.lgK); }

	          // if source is past SPARSE mode, make sure that union is a bitMatrix.
	          if ((sourceFlavorOrd > 1) && (union.accumulator != null)) {
	            union.bitMatrix = CpcUtil.bitMatrixOfSketch(union.accumulator);
	            union.accumulator = null;
	          }

	          final int state = ((sourceFlavorOrd - 1) << 1) | ((union.bitMatrix != null) ? 1 : 0);
	          switch (state) {
	            case 0 : { //A: Sparse, bitMatrix == null, accumulator valid
	              if (union.accumulator == null) {
	                //CodeQL could not figure this out so I have to insert this.
	                throw new SketchesStateException("union.accumulator can never be null here.");
	              }
	              if ((union.accumulator.getFlavor() == EMPTY)
	                  && (union.lgK == source.lgK)) {
	                union.accumulator = source.copy();
	                break;
	              }
	              walkTableUpdatingSketch(union.accumulator, source.pairTable);
	              // if the accumulator has graduated beyond sparse, switch union to a bitMatrix
	              if (union.accumulator.getFlavor().ordinal() > 1) {
	                union.bitMatrix = CpcUtil.bitMatrixOfSketch(union.accumulator);
	                union.accumulator = null;
	              }
	              break;
	            }
	            case 1 : { //B: Sparse, bitMatrix valid, accumulator == null
	              orTableIntoMatrix(union.bitMatrix, union.lgK, source.pairTable);
	              break;
	            }
	            case 3 :   //C: Hybrid, bitMatrix valid, accumulator == null
	            case 5 : { //C: Pinned, bitMatrix valid, accumulator == null
	              orWindowIntoMatrix(union.bitMatrix, union.lgK, source.slidingWindow,
	                  source.windowOffset, source.lgK);
	              orTableIntoMatrix(union.bitMatrix, union.lgK, source.pairTable);
	              break;
	            }
	            case 7 : { //D: Sliding, bitMatrix valid, accumulator == null
	              // SLIDING mode involves inverted logic, so we can't just walk the source sketch.
	              // Instead, we convert it to a bitMatrix that can be OR'ed into the destination.
	              final long[] sourceMatrix = CpcUtil.bitMatrixOfSketch(source);
	              orMatrixIntoMatrix(union.bitMatrix, union.lgK, sourceMatrix, source.lgK);
	              break;
	            }
	            default: throw new SketchesStateException("Illegal Union state: " + state);
	          }
	*/
	return nil
}

func (u *CpcUnion) checkUnionState() error {
	if u == nil {
		return fmt.Errorf("union cannot be nil")
	}

	if u.accumulator.lgK != 0 && u.bitMatrix != nil {
		return fmt.Errorf("accumulator and bitMatrix cannot be both valid or both nil")
	}
	if u.accumulator.lgK != 0 { // not nil
		if u.accumulator.numCoupons > 0 {
			if u.accumulator.slidingWindow != nil || u.accumulator.pairTable == nil {
				return fmt.Errorf("Non-empty union accumulator must be SPARSE")
			}
		}
		if u.lgK != u.accumulator.lgK {
			return fmt.Errorf("union LgK must equal accumulator LgK")
		}
	}
	return nil
}

func (u *CpcUnion) reduceUnionK(newLgK int) error {
	if newLgK < u.lgK {
		if u.bitMatrix != nil {
			// downsample the union's bit matrix
			newK := 1 << newLgK
			newMatrix := make([]uint64, newK)
			orMatrixIntoMatrix(newMatrix, newLgK, u.bitMatrix, u.lgK)
			u.bitMatrix = newMatrix
			u.lgK = newLgK
		} else {
			// downsample the union's accumulator
			oldSketch := u.accumulator
			if oldSketch.numCoupons == 0 {
				acc, err := NewCpcSketch(newLgK, oldSketch.seed)
				if err != nil {
					return err
				}
				u.accumulator = acc
				u.lgK = newLgK
				return nil
			}
			sk, err := NewCpcSketch(newLgK, oldSketch.seed)
			if err != nil {
				return err
			}
			newSketch := sk
			if err := walkTableUpdatingSketch(&newSketch, oldSketch.pairTable); err != nil {
				return err
			}
			finalNewFlavor := newSketch.GetFlavor()
			if finalNewFlavor == CpcFlavorSparse {
				u.accumulator = newSketch
				u.lgK = newLgK
				return nil
			}
			// the new sketch has graduated beyond sparse, so convert to bitMatrix
			//u.accumulator = nil
			u.bitMatrix = bitMatrixOfSketch(newSketch)
			u.lgK = newLgK
		}
	}
	return nil
}

func walkTableUpdatingSketch(dest *CpcSketch, table *pairTable) error {
	slots := table.slotsArr
	numSlots := 1 << table.lgSizeInts
	destMask := ((1<<dest.lgK)-1)<<6 | 63 // downsamples when dest.lgK < srcLgK

	stride := int(internal.InverseGolden * float64(numSlots))
	if stride == (stride >> 1 << 1) {
		stride++
	}

	for i, j := 0, 0; i < numSlots; i, j = i+1, j+stride {
		j &= numSlots - 1
		rowCol := slots[j]
		if rowCol != -1 {
			if err := dest.rowColUpdate(rowCol & destMask); err != nil {
				return err
			}
		}

	}

	return nil
}
