package kll

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestJavaCompat(t *testing.T) {
	t.Run("Java KLL String", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			digits := numDigits(n)
			bytes, err := os.ReadFile(fmt.Sprintf("%s/kll_string_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)
			sketch, err := NewItemsSketchFromSlice[string](bytes, stringItemsSketchOp{})
			if err != nil {
				return
			}

			assert.Equal(t, sketch.GetK(), uint16(200))
			if n == 0 {
				assert.True(t, sketch.IsEmpty())
			} else {
				assert.False(t, sketch.IsEmpty())
			}

			if n > 100 {
				assert.True(t, sketch.IsEstimationMode())
			} else {
				assert.False(t, sketch.IsEstimationMode())
			}

			if n > 0 {
				minV, err := sketch.GetMinItem()
				assert.NoError(t, err)
				assert.Equal(t, minV, intToFixedLengthString(1, digits))

				maxV, err := sketch.GetMaxItem()
				assert.NoError(t, err)
				assert.Equal(t, maxV, intToFixedLengthString(n, digits))
			}

			/*
			   assertEquals(sketch.getK(), 200);
			   assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
			   assertTrue(n > 100 ? sketch.isEstimationMode() : !sketch.isEstimationMode());
			   assertEquals(sketch.getN(), n);
			   if (n > 0) {
			     assertEquals(sketch.getMinItem(), Integer.toString(1));
			     assertEquals(sketch.getMaxItem(), Integer.toString(n));
			     long weight = 0;
			     QuantilesGenericSketchIterator<String> it = sketch.iterator();
			     while (it.next()) {
			       assertTrue(numericOrder.compare(it.getQuantile(), sketch.getMinItem()) >= 0);
			       assertTrue(numericOrder.compare(it.getQuantile(), sketch.getMaxItem()) <= 0);
			       weight += it.getWeight();
			     }
			     assertEquals(weight, n);
			   }
			*/

		}
	})
}
