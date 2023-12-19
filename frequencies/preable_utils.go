package frequencies

const (

	// emptyFlagMask flag bit masks
	// due to a mistake different bits were used in C++ and Java to indicate empty sketch
	// therefore both are set and checked for compatibility with historical binary format
	emptyFlagMask = 5

	serVer = 1
)
