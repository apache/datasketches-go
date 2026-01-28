<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

[![Go](https://github.com/apache/datasketches-go/actions/workflows/go.yml/badge.svg)](https://github.com/apache/datasketches-go/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/apache/datasketches-go)](https://goreportcard.com/report/github.com/apache/datasketches-go)
[![Release](https://img.shields.io/github/release/apache/datasketches-go.svg)](https://github.com/apache/datasketches-go/releases)
[![GoDoc](https://godoc.org/github.com/apache/datasketches-go?status.svg)](https://godoc.org/github.com/apache/datasketches-go)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/apache/datasketches-go/blob/master/LICENSE)
[![Coverage Status](https://coveralls.io/repos/github/apache/datasketches-go/badge.svg?branch=main)](https://coveralls.io/github/apache/datasketches-go?branch=main)

# Apache<sup>&reg;</sup> DataSketches&trade; Core Go Library Component
This is the core Go component of the DataSketches library.  It contains some of the sketching algorithms and can be accessed directly from user applications.

Note that we have a parallel core component for C++, Java and Python implementations of the same sketch algorithms,
[datasketches-cpp](https://github.com/apache/datasketches-cpp) and [datasketches-java](https://github.com/apache/datasketches-java).

Please visit the main [DataSketches website](https://datasketches.apache.org) for more information.

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.



## Major Sketches
| Type         | Implementation          | Status |
|--------------|-------------------------|--|
| Cardinality	 |                         |  |
| 	            | CpcSketch               | ⚠️ |
| 	            | HllSketch               | ⚠️ |
| 	            | ThetaSketch             | ⚠️ |
| 	            | TupleSketch<S>          | ⚠️ |
| Quantiles	   |                         |  |
| 	            | CormodeDoublesSketch    | ❌ |
| 	            | CormodeItemsSketch<T>   | ❌ |
| 	            | KllDoublesSketch        | ❌ |
| 	            | KllFloatsSketch         | ❌ |
| 	            | KllSketch<T>            | ⚠️ |
| 	            | ReqFloatsSketch         | ❌ |
| 	            | TDigestDouble           | ⚠️ |
| Frequencies  |                         | ️ |
|              | FreqLongsSketch         | ⚠️ |
|              | FreqItemsSketch<T>      | ⚠️ |
|              | CountMinSketch          | ⚠️ |
| Sampling |                         |  |
|  | ReservoirLongsSketch    | ❌ |
|  | ReserviorItemsSketch<T> | ⚠️ |
| 	  | VarOptItemsSketch<T>    | 🚧 |
| Membership |                         | |
| | BloomFilter             | ⚠️ |


## Specialty Sketches
| Type | Interface Name | Status |
| --- | --- |---|
| Cardinality/FM85 | UniqueCountMap  | ❌ |
| Cardinality/Tuple	| | |
| 	| FdtSketch | ❌ |
| 	| ArrayOfDoublesSketch  | ⚠️ |
| 	| DoubleSketch  | ❌ |
| 	| IntegerSketch  | ❌ |
|	| ArrayOfStringsSketch | ❌ |
| 	| EngagementTest3 | ❌ |


❌ = Not yet implemented

⚠️ = Implemented but not officially released

🚧 = In progress

# Build & Runtime Dependencies

This code requires Go 1.24

# Compilation and Test 

Test can be run using go test command 
```
go test ./...
```

## Experimental build tags

Some packages (e.g., `sampling.VarOptItemsSketch`) are guarded by the `experimental`
build tag. VarOptItemsSketch is currently under development. To include them in builds or tests, pass the tag explicitly:
```
go build -tags=experimental ./...
go test -tags=experimental ./...
```

A Dockerfile is also provided with the necessary env to build and test the project.

```
./build/Dockerfile
./build/run-docker-test.sh
```
