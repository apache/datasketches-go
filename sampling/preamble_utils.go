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

import "fmt"

const (
	varOptPreambleLongsEmpty  = 1
	varOptPreambleLongsWarmup = 3
	varOptPreambleLongsFull   = 4
	varOptSerVer              = 2
	varOptFlagEmpty           = 0x04
	varOptFlagGadget          = 0x80
)

func encodeVarOptResizeFactor(rf ResizeFactor) (byte, error) {
	switch rf {
	case ResizeX1:
		return 0x00, nil
	case ResizeX2:
		return 0x40, nil
	case ResizeX4:
		return 0x80, nil
	case ResizeX8:
		return 0xC0, nil
	default:
		return 0, fmt.Errorf("unsupported resize factor: %d", rf)
	}
}

func decodeVarOptResizeFactor(bits byte) (ResizeFactor, error) {
	switch bits & 0x03 {
	case 0:
		return ResizeX1, nil
	case 1:
		return ResizeX2, nil
	case 2:
		return ResizeX4, nil
	case 3:
		return ResizeX8, nil
	default:
		return ResizeX8, fmt.Errorf("invalid resize factor bits: %d", bits)
	}
}
