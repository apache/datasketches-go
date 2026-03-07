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

package tuple

import (
	"fmt"
	"unicode/utf8"
)

type int32Summary struct {
	value int32
}

func (s *int32Summary) Reset() {
	s.value = 0
}

func (s *int32Summary) Clone() Summary {
	return &int32Summary{
		value: s.value,
	}
}

func (s *int32Summary) Update(value int32) {
	s.value += value
}

func newInt32Summary() *int32Summary {
	return &int32Summary{}
}

type float64Summary struct {
	value float64
}

func (s *float64Summary) Reset() {
	s.value = 0
}

func (s *float64Summary) Clone() Summary {
	return &float64Summary{value: s.value}
}

func (s *float64Summary) Update(value float64) {
	s.value += value
}

func newFloat64Summary() *float64Summary {
	return &float64Summary{}
}

type int32ValueSummary struct {
	value int32
}

func (s int32ValueSummary) Reset()             {}
func (s int32ValueSummary) Clone() Summary     { return s }
func (s int32ValueSummary) Update(value int32) {}

func newInt32ValueSummary() int32ValueSummary {
	return int32ValueSummary{}
}

type stringSummary struct {
	value string
}

func (s *stringSummary) Reset() {
	s.value = ""
}

func (s *stringSummary) Clone() Summary {
	return &stringSummary{value: s.value}
}

func (s *stringSummary) Update(value string) {
	s.value = value
}

func (s *stringSummary) ValidateBeforeEncode() error {
	if !utf8.ValidString(s.value) {
		return fmt.Errorf("invalid UTF-8 string")
	}
	return nil
}

func (s *stringSummary) ValidateAfterDecode() error {
	if !utf8.ValidString(s.value) {
		return fmt.Errorf("invalid UTF-8 string")
	}
	return nil
}

func newStringSummary() *stringSummary {
	return &stringSummary{}
}
