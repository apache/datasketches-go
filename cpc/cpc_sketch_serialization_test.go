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
