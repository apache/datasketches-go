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

package frequencies

type ErrorType struct {
	id   int
	Name string
}

type ErrorTypes struct {
	NO_FALSE_POSITIVES ErrorType
	NO_FALSE_NEGATIVES ErrorType
}

var ErrorTypeEnum = &ErrorTypes{
	NO_FALSE_POSITIVES: ErrorType{
		id:   1,
		Name: "NO_FALSE_POSITIVES",
	},
	NO_FALSE_NEGATIVES: ErrorType{
		id:   2,
		Name: "NO_FALSE_NEGATIVES",
	},
}
