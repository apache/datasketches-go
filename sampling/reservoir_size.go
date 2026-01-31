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

package sampling

import "errors"

const (
	reservoirSizeBinsPerOctave    = 2048
	reservoirSizeInvBinsPerOctave = 1.0 / reservoirSizeBinsPerOctave
	reservoirSizeExponentMask     = 0x1F
	reservoirSizeExponentShift    = 11
	reservoirSizeIndexMask        = 0x07FF
	reservoirSizeMaxEncValue      = 0xF7FF // p=30, i=2047
)

func decodeReservoirSize(encoded uint16) (int, error) {
	value := int(encoded)
	if value > reservoirSizeMaxEncValue {
		return 0, errors.New("invalid encoded reservoir size")
	}

	p := (value >> reservoirSizeExponentShift) & reservoirSizeExponentMask
	i := value & reservoirSizeIndexMask

	base := 1 << uint(p)
	return int(float64(base) * ((float64(i) * reservoirSizeInvBinsPerOctave) + 1.0)), nil
}
