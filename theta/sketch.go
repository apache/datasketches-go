package theta

import "iter"

// Sketch is a generalization of the Kth Minimum Value (KMV) sketch.
type Sketch interface {
	// IsEmpty returns true if this sketch represents an empty set
	// (the same as no retained entries!)
	IsEmpty() bool

	// Estimate returns estimate of the distinct count of the input stream
	Estimate() float64

	// LowerBound returns the approximate lower error bound given a number of standard deviations.
	// This parameter is similar to the number of standard deviations of the normal distribution
	// and corresponds to approximately 67%, 95% and 99% confidence intervals.
	// numStdDevs number of Standard Deviations (1, 2 or 3)
	LowerBound(numStdDevs uint8) (float64, error)

	// UpperBound returns the approximate upper error bound given a number of standard deviations.
	// This parameter is similar to the number of standard deviations of the normal distribution
	// and corresponds to approximately 67%, 95% and 99% confidence intervals.
	// numStdDevs number of Standard Deviations (1, 2 or 3)
	UpperBound(numStdDevs uint8) (float64, error)

	// IsEstimationMode returns true if the sketch is in estimation mode
	// (as opposed to exact mode)
	IsEstimationMode() bool

	// Theta returns theta as a fraction from 0 to 1 (effective sampling rate)
	Theta() float64

	// Theta64 returns theta as a positive integer between 0 and math.MaxInt64
	Theta64() uint64

	// NumRetained returns the number of retained entries in the sketch
	NumRetained() uint32

	// SeedHash returns hash of the seed that was used to hash the input
	SeedHash() (uint16, error)

	// IsOrdered returns true if retained entries are ordered
	IsOrdered() bool

	// String returns a human-readable summary of this sketch as a string
	// If shouldPrintItems is true, include the list of items retained by the sketch
	String(shouldPrintItems bool) string

	// Iter returns an iterator over hash values in the sketch.
	Iter() iter.Seq[uint64]
}
