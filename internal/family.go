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

package internal

type Family struct {
	Id          int
	MaxPreLongs int
}

type families struct {
	HLL       Family
	Frequency Family
	Kll       Family
	CPC       Family
}

var FamilyEnum = &families{
	HLL: Family{
		Id:          7,
		MaxPreLongs: 1,
	},
	Frequency: Family{
		Id:          10,
		MaxPreLongs: 4,
	},
	Kll: Family{
		Id:          15,
		MaxPreLongs: 2,
	},
	CPC: Family{
		Id:          16,
		MaxPreLongs: 5,
	},
}
