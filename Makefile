# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: test
test:
	rm -rf coverage.out
	go test ./... -coverprofile=coverage.out

.PHONY: generate-go-snapshots
generate-go-snapshots:
	DSKETCH_TEST_GENERATE_GO=1 go test ./... -run '^TestGenerateGoSnapshots' -count=1

.PHONY: lint
lint:
	gofmt -l .
	go tool goimports -l .
	go vet ./...

.PHONY: format
format:
	gofmt -w .
	go tool goimports -w .

.PHONY: lint-check
lint-check:
	test -z "$$(gofmt -l .)"
	test -z "$$(go tool goimports -l .)"
	go vet ./...
