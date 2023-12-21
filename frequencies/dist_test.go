package frequencies

import (
	"math"
	"math/rand"
)

func randomGeometricDist(prob float64) int64 {
	if prob <= 0.0 || prob >= 1.0 {
		panic("prob must be in (0, 1)")
	}
	return int64(1 + math.Log(rand.Float64())/math.Log(1.0-prob))
}
