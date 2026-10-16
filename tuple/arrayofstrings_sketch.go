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
	"slices"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

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
//
// It is compatible with Java's tuple Util.stringArrHash: the elements are joined
// with "," and the joined string is hashed with XXH64 as UTF-16 code units in
// little-endian byte order (UTF-16LE, no byte-order mark).
//
// Key arrays are hashed by joining their elements with ",". Keys that join to
// the same string, for example {"a,b"} and {"a", "b"}, or {} and {""}, are
// treated as the same key. If key elements may contain commas, escape or encode
// them before updating the sketch.
//
// The strings are expected to be valid UTF-8. Invalid UTF-8 is replaced with
// U+FFFD the same way Java's UTF-8 decoder (new String(bytes, UTF_8)) replaces
// it, so a key hashes as Java hashes the String decoded from the same bytes.
func GenerateHashKeyFromStrings(s []string) uint64 {
	var stack [256]byte
	buf := stack[:0]
	for i, v := range s {
		if i > 0 {
			buf = append(buf, ',', 0)
		}
		buf = appendUTF16LE(buf, v)
	}

	var d xxhash.Digest
	d.ResetWithSeed(stringSliceHashSeed)
	d.Write(buf) //nolint:errcheck
	return d.Sum64()
}

// appendUTF16LE appends s to dst as UTF-16LE code units, which is how Java hashes
// a String with XxHash.hashCharArr(s.toCharArray(), ...).
//
// s is decoded from UTF-8. Malformed input is replaced with U+FFFD exactly as
// Java's UTF-8 decoder does it: one U+FFFD for the longest valid prefix of a
// malformed sequence (at least one byte), and one U+FFFD for a complete 3-byte
// sequence that encodes a surrogate. Ranging over s would instead yield one
// U+FFFD per invalid byte.
func appendUTF16LE(dst []byte, s string) []byte {
	for i := 0; i < len(s); {
		lead := s[i]
		i++
		if lead < utf8.RuneSelf {
			dst = binary.LittleEndian.AppendUint16(dst, uint16(lead))
			continue
		}
		numContinuation, secondMin, secondMax := utf8SequenceRule(lead)
		if numContinuation == 0 {
			dst = binary.LittleEndian.AppendUint16(dst, unicode.ReplacementChar)
			continue
		}
		r := rune(lead & (0x3f >> numContinuation))
		numRead := 0
		for numRead < numContinuation && i < len(s) {
			b := s[i]
			var valid bool
			if numRead == 0 {
				valid = b >= secondMin && b <= secondMax
			} else {
				valid = b&0xc0 == 0x80
			}
			if !valid {
				break
			}
			r = r<<6 | rune(b&0x3f)
			i++
			numRead++
		}
		if numRead < numContinuation || utf16.IsSurrogate(r) {
			// The bytes read so far are the longest valid prefix. Resume at the next byte.
			dst = binary.LittleEndian.AppendUint16(dst, unicode.ReplacementChar)
			continue
		}
		if r1, r2 := utf16.EncodeRune(r); r1 != unicode.ReplacementChar {
			dst = binary.LittleEndian.AppendUint16(dst, uint16(r1))
			dst = binary.LittleEndian.AppendUint16(dst, uint16(r2))
			continue
		}
		dst = binary.LittleEndian.AppendUint16(dst, uint16(r))
	}
	return dst
}

// utf8SequenceRule returns the number of continuation bytes that follow a UTF-8
// lead byte, or 0 if the byte cannot start a sequence (0x80-0xC1, 0xF5-0xFF),
// and the range the second byte must be in, as Java's decoder checks it.
func utf8SequenceRule(lead byte) (numContinuation int, secondMin, secondMax byte) {
	switch {
	case lead >= 0xc2 && lead <= 0xdf:
		return 1, 0x80, 0xbf
	case lead == 0xe0:
		return 2, 0xa0, 0xbf // no overlong forms
	case lead >= 0xe1 && lead <= 0xef:
		// ED A0-BF encodes a surrogate, which Java's decoder accepts here and
		// replaces once decoded.
		return 2, 0x80, 0xbf
	case lead == 0xf0:
		return 3, 0x90, 0xbf // no overlong forms
	case lead >= 0xf1 && lead <= 0xf3:
		return 3, 0x80, 0xbf
	case lead == 0xf4:
		return 3, 0x80, 0x8f // nothing above U+10FFFF
	}
	return 0, 0, 0
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
	s.values = slices.Clone(values)
}

// ArrayOfStringsSummaryWriter writes an ArrayOfStringsSummary to the provided io.Writer in binary format.
// It validates the length of the string slice and computes total bytes for serialization.
// Returns an error if the input exceeds the maximum allowed slice length or if any write operation fails.
//
// If the caller cares about cross-language compatibility,
// it is the caller's responsibility to ensure that those strings are encoded as valid UTF-8.
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
//
// If the caller cares about cross-language compatibility,
// it is the caller's responsibility to ensure that the serialized string data is encoded as valid UTF-8.
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
//
// If the caller cares about cross-language compatibility,
// it is the caller's responsibility to ensure that those strings are encoded as valid UTF-8.
func ArrayOfStringsInlineSummaryWriter(w io.Writer, summary ArrayOfStringsInlineSummary) error {
	return writeStrings(w, summary.values)
}

// ArrayOfStringsInlineSummaryReader reads an ArrayOfStringsInlineSummary from the provided io.Reader in binary format.
// It validates the length of the string slice and reads the total bytes for deserialization.
// Returns an error if the input exceeds the maximum allowed slice length or if any read operation fails.
//
// If the caller cares about cross-language compatibility,
// it is the caller's responsibility to ensure that the serialized string data is encoded as valid UTF-8.
func ArrayOfStringsInlineSummaryReader(r io.Reader) (ArrayOfStringsInlineSummary, error) {
	values, err := readStrings(r)
	if err != nil {
		return ArrayOfStringsInlineSummary{}, err
	}

	return ArrayOfStringsInlineSummary{
		values: values,
	}, nil
}
