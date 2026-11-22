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

package count

import (
	"errors"
	"fmt"
	"math"

	"github.com/apache/datasketches-go/internal"
)

func SuggestNumBuckets(relativeError float64) (int32, error) {
	if relativeError <= 0 {
		return 0, errors.New("relative error must be greater than 0.0")
	}
	return int32(math.Ceil(math.Exp(1.0) / relativeError)), nil
}

func SuggestNumHashes(confidence float64) (uint8, error) {
	if confidence < 0 || confidence > 1.0 {
		return 0, errors.New("confidence must be between 0 and 1.0 (inclusive)")
	}
	return uint8(max(1.0, min(math.Ceil(math.Log(1.0/(1.0-confidence))), math.MaxUint8))), nil
}

func checkHeaderValidity(preambleLongs, serVer, familyID byte) error {
	if preambleLongs != PreambleLongsShort {
		return fmt.Errorf("possible corruption: preamble longs must be %d: %d", PreambleLongsShort, preambleLongs)
	}
	if serVer != SerialVersion1 {
		return fmt.Errorf("possible corruption: serial version must be %d: %d", SerialVersion1, serVer)
	}
	if int(familyID) != internal.FamilyEnum.CountMinSketch.Id {
		return fmt.Errorf("possible corruption: family ID must be %d: %d", internal.FamilyEnum.CountMinSketch.Id, familyID)
	}
	return nil
}

const (
	PreambleLongsShort = 2
	SerialVersion1     = 1
	Null8              = 0
	Null32             = 0
	IsEmpty            = 0
)
