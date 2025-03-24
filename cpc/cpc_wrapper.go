package cpc

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
)

// CpcWrapper is a read-only view of a serialized CPC sketch.
type CpcWrapper struct {
	mem []byte
}

// NewCpcWrapperFromBytes constructs a read-only view of the given byte array
// that contains a CpcSketch. It checks that the preamble is valid and that
// the sketch is in compressed format.
func NewCpcWrapperFromBytes(byteArray []byte) (*CpcWrapper, error) {
	if err := checkLoPreamble(byteArray); err != nil {
		return nil, fmt.Errorf("CpcWrapper: preamble check failed: %w", err)
	}
	if !isCompressed(byteArray) {
		return nil, fmt.Errorf("CpcWrapper: sketch is not compressed")
	}
	return &CpcWrapper{mem: byteArray}, nil
}

// GetEstimate returns the best estimate of the cardinality of the wrapped sketch.
// If the sketch is in HIP mode, we return the HIP accumulator; otherwise we
// return the ICON estimate.
func (cw *CpcWrapper) GetEstimate() float64 {
	if !hasHip(cw.mem) {
		return iconEstimate(getLgK(cw.mem), getNumCoupons(cw.mem))
	}
	return getHipAccum(cw.mem)
}

// GetLgK returns the log-base-2 of K for this sketch.
func (cw *CpcWrapper) GetLgK() int {
	return getLgK(cw.mem)
}

// GetLowerBound returns the lower bound of the confidence interval given kappa
// (the number of standard deviations from the mean).
func (cw *CpcWrapper) GetLowerBound(kappa int) float64 {
	if !hasHip(cw.mem) {
		return iconConfidenceLB(getLgK(cw.mem), getNumCoupons(cw.mem), kappa)
	}
	return hipConfidenceLB(getLgK(cw.mem), getNumCoupons(cw.mem), getHipAccum(cw.mem), kappa)
}

// GetUpperBound returns the upper bound of the confidence interval given kappa
// (the number of standard deviations from the mean).
func (cw *CpcWrapper) GetUpperBound(kappa int) float64 {
	if !hasHip(cw.mem) {
		return iconConfidenceUB(getLgK(cw.mem), getNumCoupons(cw.mem), kappa)
	}
	return hipConfidenceUB(getLgK(cw.mem), getNumCoupons(cw.mem), getHipAccum(cw.mem), kappa)
}

// GetFamily returns the family ID for CPC sketches.
func (cw *CpcWrapper) GetFamily() int {
	return internal.FamilyEnum.CPC.Id
}
