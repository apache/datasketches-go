package frequencies

type ErrorType = int

const (
	NO_FALSE_POSITIVES = ErrorType(1)
	NO_FALSE_NEGATIVES = ErrorType(2)
)
