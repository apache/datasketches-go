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

// Package testutil holds helpers shared by tests. Only _test.go files may import it,
// so that the testing package is never linked into library code.
package testutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/apache/datasketches-go/internal"
)

// ReadSnapshot reads a cross-language serialization snapshot. If the file is
// missing, it fails the test with instructions for getting the snapshot data.
func ReadSnapshot(t testing.TB, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatal(missingSnapshotMessage(path))
	}
	if err != nil {
		t.Fatalf("read snapshot %s: %v", path, err)
	}
	return b
}

// missingSnapshotMessage explains how to get the snapshot at path.
func missingSnapshotMessage(path string) string {
	command := "make download-snapshots"
	if strings.HasPrefix(path, internal.GoPath) {
		command = "make generate-go-snapshots"
	}
	return fmt.Sprintf("serialization snapshot not found: %s\n"+
		"Run `%s` from the repository root to get it (see \"Compilation and Test\" in README.md).",
		path, command)
}
