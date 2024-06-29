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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCPCCheckUpdatesEstimate(t *testing.T) {
	sk, err := NewCpcSketch(10, 0)
	assert.NoError(t, err)
	assert.Equal(t, sk.getFormat(), format_empty_hip)
	err = sk.UpdateUint64(1)
	assert.NoError(t, err)
	err = sk.UpdateFloat64(2.0)
	assert.NoError(t, err)
	err = sk.UpdateString("3")
	assert.NoError(t, err)
	bytes := []byte{4, 4}
	err = sk.UpdateSlice(bytes)
	assert.NoError(t, err)
}

/*
  @Test
  public void checkUpdatesEstimate() {
    final CpcSketch sk = new CpcSketch(10, 0);
    println(sk.toString(true));
    assertEquals(sk.getFormat(), Format.EMPTY_HIP);
    sk.update(1L);
    sk.update(2.0);
    sk.update("3");
    byte[] bytes = new byte[] { 4, 4 };
    sk.update(bytes);
    sk.update(ByteBuffer.wrap(bytes));  // same as previous
    sk.update(ByteBuffer.wrap(bytes, 0, 1));
    sk.update(new char[] { 5 });
    sk.update(new int[] { 6 });
    sk.update(new long[] { 7 });
    final double est = sk.getEstimate();
    final double lb = sk.getLowerBound(2);
    final double ub = sk.getUpperBound(2);
    assertTrue(lb >= 0);
    assertTrue(lb <= est);
    assertTrue(est <= ub);
    assertEquals(sk.getFlavor(), Flavor.SPARSE);
    assertEquals(sk.getFormat(), Format.SPARSE_HYBRID_HIP);
    println(sk.toString());
    println(sk.toString(true));
  }
*/
