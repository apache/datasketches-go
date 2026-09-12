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

package kll

import "errors"

type sketchStructure struct {
	preInts int
	serVer  int
}

var (
	_COMPACT_EMPTY  = sketchStructure{_PREAMBLE_INTS_EMPTY_SINGLE, _SERIAL_VERSION_EMPTY_FULL}
	_COMPACT_SINGLE = sketchStructure{_PREAMBLE_INTS_EMPTY_SINGLE, _SERIAL_VERSION_SINGLE}
	_COMPACT_FULL   = sketchStructure{_PREAMBLE_INTS_FULL, _SERIAL_VERSION_EMPTY_FULL}
	_UPDATABLE      = sketchStructure{_PREAMBLE_INTS_FULL, _SERIAL_VERSION_UPDATABLE}
)

func (s sketchStructure) getPreInts() int { return s.preInts }

func (s sketchStructure) getSerVer() int { return s.serVer }

func getSketchStructure(preInts, serVer int) (sketchStructure, error) {
	switch preInts {
	case _PREAMBLE_INTS_EMPTY_SINGLE:
		switch serVer {
		case _SERIAL_VERSION_EMPTY_FULL:
			return _COMPACT_EMPTY, nil
		case _SERIAL_VERSION_SINGLE:
			return _COMPACT_SINGLE, nil
		}
	case _PREAMBLE_INTS_FULL:
		switch serVer {
		case _SERIAL_VERSION_EMPTY_FULL:
			return _COMPACT_FULL, nil
		case _SERIAL_VERSION_UPDATABLE:
			return _UPDATABLE, nil
		}
	default:
		return sketchStructure{}, errors.New("invalid preamble ints and serial version combo")
	}
}
