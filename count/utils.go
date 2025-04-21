package count

import (
	"errors"
	"math"

	"golang.org/x/exp/constraints"
)

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func SuggestNumBuckets(relativeError float64) (int32, error) {
	if relativeError <= 0 {
		return 0, errors.New("relative error must be greater than 0.0")
	}
	return int32(math.Ceil(math.Exp(1.0) / relativeError)), nil
}

func SuggestNumHashes(confidence float64) (int8, error) {
	if confidence < 0 || confidence > 1.0 {
		return 0, errors.New("confidence must be between 0 and 1.0 (inclusive)")
	}
	return Min(int8(math.Ceil(math.Log(1.0/(1.0-confidence)))), int8(math.MaxInt8)), nil
}
