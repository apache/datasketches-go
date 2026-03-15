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
	"encoding/binary"
	"errors"
	"io"

	"github.com/cespare/xxhash/v2"
)

var (
	ErrTooManyStrings = errors.New("string slice length must be <= 127")
)

const (
	// NOTE: This is from java Util.PRIME for array of strings sketch key hash.
	stringSliceHashSeed = 0x7A3C_CA71

	maxStringSliceLength = 127
)

// GenerateHashKeyFromStrings returns the hash of the concatenated strings.
func GenerateHashKeyFromStrings(s []string) uint64 {
	hasher := xxhash.NewWithSeed(stringSliceHashSeed)
	for i, v := range s {
		hasher.WriteString(v)
		if i+1 < len(s) {
			hasher.WriteString(",")
		}
	}
	return hasher.Sum64()
}

// ArrayOfStringsSummary represents a summary type that holds and manages a collection of string values.
type ArrayOfStringsSummary struct {
	values []string
}

// NewArrayOfStringsSummaryFunc creates and returns a pointer to a new instance of ArrayOfStringsSummary.
func NewArrayOfStringsSummaryFunc() *ArrayOfStringsSummary {
	return &ArrayOfStringsSummary{}
}

// Reset clears all string values from the ArrayOfStringsSummary.
func (s *ArrayOfStringsSummary) Reset() {
	s.values = s.values[:0]
}

// Clone creates and returns a deep copy of the current ArrayOfStringsSummary instance.
func (s *ArrayOfStringsSummary) Clone() Summary {
	return &ArrayOfStringsSummary{
		values: append([]string{}, s.values...),
	}
}

// Update incorporates a new string value into the summary.
func (s *ArrayOfStringsSummary) Update(values []string) {
	s.values = values
}

// ArrayOfStringsSummaryWriter writes an ArrayOfStringsSummary to the provided io.Writer in binary format.
// It validates the length of the string slice and computes total bytes for serialization.
// Returns an error if the input exceeds the maximum allowed slice length or if any write operation fails.
func ArrayOfStringsSummaryWriter(w io.Writer, summary *ArrayOfStringsSummary) error {
	return writeStrings(w, summary.values)
}

func writeStrings(w io.Writer, values []string) error {
	if len(values) > maxStringSliceLength {
		return ErrTooManyStrings
	}

	totalBytes := computeStringsTotalBytes(values)
	if err := binary.Write(w, binary.LittleEndian, totalBytes); err != nil {
		return err
	}

	numNodes := uint8(len(values))
	if err := binary.Write(w, binary.LittleEndian, numNodes); err != nil {
		return err
	}

	for _, v := range values {
		length := uint32(len(v))
		if err := binary.Write(w, binary.LittleEndian, length); err != nil {
			return err
		}

		if _, err := w.Write([]byte(v)); err != nil {
			return err
		}
	}
	return nil
}

func computeStringsTotalBytes(values []string) uint32 {
	total := uint32(4 + 1 + len(values)*4)
	for _, v := range values {
		total += uint32(len(v))
	}
	return total
}

// ArrayOfStringsSummaryReader reads an ArrayOfStringsSummary from the provided io.Reader in binary format.
// It validates the length of the string slice and reads the total bytes for deserialization.
// Returns an error if the input exceeds the maximum allowed slice length or if any read operation fails.
func ArrayOfStringsSummaryReader(r io.Reader) (*ArrayOfStringsSummary, error) {
	values, err := readStrings(r)
	if err != nil {
		return nil, err
	}

	return &ArrayOfStringsSummary{
		values: values,
	}, nil
}

func readStrings(r io.Reader) ([]string, error) {
	var totalBytes uint32
	if err := binary.Read(r, binary.LittleEndian, &totalBytes); err != nil {
		return nil, err
	}

	var numNodes uint8
	if err := binary.Read(r, binary.LittleEndian, &numNodes); err != nil {
		return nil, err
	}
	if numNodes > maxStringSliceLength {
		return nil, ErrTooManyStrings
	}

	values := make([]string, 0, numNodes)
	for i := uint8(0); i < numNodes; i++ {
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return nil, err
		}

		bytes := make([]byte, length)
		if _, err := io.ReadFull(r, bytes); err != nil {
			return nil, err
		}

		values = append(values, string(bytes))
	}

	return values, nil
}

// ArrayOfStringsInlineSummary stores a string-slice summary inline in the sketch entry
// instead of behind a pointer to reduce per-entry allocations.
type ArrayOfStringsInlineSummary struct {
	values []string
}

// Reset clears the inline summary state.
// It is a no-op because this type is intended to be updated via ArrayOfStringsInlineSummaryUpdateFunc.
func (s ArrayOfStringsInlineSummary) Reset() {}

// Update satisfies the UpdatableSummary interface.
// It is a no-op because this type is intended to be updated via ArrayOfStringsInlineSummaryUpdateFunc.
func (s ArrayOfStringsInlineSummary) Update([]string) {}

// Clone returns a copy of the inline summary.
func (s ArrayOfStringsInlineSummary) Clone() Summary {
	return ArrayOfStringsInlineSummary{
		values: append([]string(nil), s.values...),
	}
}

// NewArrayOfStringsInlineSummary returns a new empty inline summary.
func NewArrayOfStringsInlineSummary() ArrayOfStringsInlineSummary {
	return ArrayOfStringsInlineSummary{}
}

// ArrayOfStringsInlineSummaryUpdateFunc updates an inline summary with the provided string slice.
// The returned summary stores the slice header inline and reuses the provided slice reference.
func ArrayOfStringsInlineSummaryUpdateFunc(s ArrayOfStringsInlineSummary, values []string) ArrayOfStringsInlineSummary {
	return ArrayOfStringsInlineSummary{
		values: values,
	}
}

// ArrayOfStringsInlineSummaryWriter writes an ArrayOfStringsInlineSummary to the provided io.Writer in binary format.
// It validates the length of the string slice and computes total bytes for serialization.
// Returns an error if the input exceeds the maximum allowed slice length or if any write operation fails.
func ArrayOfStringsInlineSummaryWriter(w io.Writer, summary ArrayOfStringsInlineSummary) error {
	return writeStrings(w, summary.values)
}

// ArrayOfStringsInlineSummaryReader reads an ArrayOfStringsInlineSummary from the provided io.Reader in binary format.
// It validates the length of the string slice and reads the total bytes for deserialization.
// Returns an error if the input exceeds the maximum allowed slice length or if any read operation fails.
func ArrayOfStringsInlineSummaryReader(r io.Reader) (ArrayOfStringsInlineSummary, error) {
	values, err := readStrings(r)
	if err != nil {
		return ArrayOfStringsInlineSummary{}, err
	}

	return ArrayOfStringsInlineSummary{
		values: values,
	}, nil
}
