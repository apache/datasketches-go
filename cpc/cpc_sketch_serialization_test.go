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

package cpc

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestJavaCompat(t *testing.T) {
	t.Run("Java CPC", func(t *testing.T) {
		nArr := []int{0, 100, 200, 2000, 20000}
		flavorArr := []CpcFlavor{CpcFlavorEmpty, CpcFlavorSparse, CpcFlavorHybrid, CpcFlavorPinned, CpcFlavorSliding}
		for flavorIdx, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/cpc_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)
			sketch, err := NewCpcSketchFromSliceWithDefault(bytes)
			assert.NoError(t, err)
			assert.Equal(t, sketch.GetFlavor(), flavorArr[flavorIdx])
			assert.InDelta(t, float64(n), sketch.GetEstimate(), float64(n)*0.02)

		}
	})
}
