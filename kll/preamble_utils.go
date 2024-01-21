package kll

import "encoding/binary"

const (
	_PREAMBLE_INTS_BYTE_ADR = 0
	_SER_VER_BYTE_ADR       = 1
	_FAMILY_BYTE_ADR        = 2
	_FLAGS_BYTE_ADR         = 3
	_K_SHORT_ADR            = 4 // to 5
	_M_BYTE_ADR             = 6

	// SINGLE ITEM ONLY
	_DATA_START_ADR_SINGLE_ITEM = 8 //also ok for empty

	// MULTI-ITEM
	_N_LONG_ADR      = 8  // to 15
	_MIN_K_SHORT_ADR = 16 // to 17

	// 19 is reserved for future use
	_DATA_START_ADR = 20 // Full Sketch, not single item

	// Other static members
	_SERIAL_VERSION_EMPTY_FULL  = 1 // Empty or full preamble, NOT single item format, NOT updatable
	_SERIAL_VERSION_SINGLE      = 2 // only single-item format, NOT updatable
	_SERIAL_VERSION_UPDATABLE   = 3 // PreInts=5, Full preamble + LevelsArr + min, max + empty space
	_PREAMBLE_INTS_EMPTY_SINGLE = 2 // for empty or single item
	_PREAMBLE_INTS_FULL         = 5 // Full preamble, not empty nor single item.

	// Flag bit masks
	_EMPTY_BIT_MASK             = 1
	_LEVEL_ZERO_SORTED_BIT_MASK = 2
	_SINGLE_ITEM_BIT_MASK       = 4
)

func getPreInts(mem []byte) int {
	return int(mem[_PREAMBLE_INTS_BYTE_ADR] & 0xFF)
}

func getSerVer(mem []byte) int {
	return int(mem[_SER_VER_BYTE_ADR] & 0xFF)
}

func getFamilyID(mem []byte) int {
	return int(mem[_FAMILY_BYTE_ADR] & 0xFF)
}

func getFlags(mem []byte) int {
	return int(mem[_FLAGS_BYTE_ADR] & 0xFF)
}

func getEmptyFlag(mem []byte) bool {
	return (getFlags(mem) & _EMPTY_BIT_MASK) != 0
}

func getK(mem []byte) uint16 {
	return uint16(mem[_K_SHORT_ADR]) & 0xFFFF
}

func getM(mem []byte) uint8 {
	return mem[_M_BYTE_ADR] & 0xFF
}

func getN(mem []byte) uint64 {
	return binary.LittleEndian.Uint64(mem[_N_LONG_ADR : _N_LONG_ADR+8])
}

func getMinK(mem []byte) uint16 {
	return binary.LittleEndian.Uint16(mem[_MIN_K_SHORT_ADR : _MIN_K_SHORT_ADR+2])
}

func getNumLevels(mem []byte) uint8 {
	return mem[_FLAGS_BYTE_ADR] & 0xFF
}

/*
  static int getMemoryNumLevels(final Memory mem) {
    return mem.getByte(NUM_LEVELS_BYTE_ADR) & 0XFF;
  }
*/

func getLevelZeroSortedFlag(mem []byte) bool {
	return (getFlags(mem) & _LEVEL_ZERO_SORTED_BIT_MASK) != 0
}

/*







  static boolean getMemoryEmptyFlag(final Memory mem) {
    return (getMemoryFlags(mem) & EMPTY_BIT_MASK) != 0;
  }

  static boolean getMemoryLevelZeroSortedFlag(final Memory mem) {
    return (getMemoryFlags(mem) & LEVEL_ZERO_SORTED_BIT_MASK) != 0;
  }

  static int getMemoryK(final Memory mem) {
    return mem.getShort(K_SHORT_ADR) & 0XFFFF;
  }

  static int getMemoryM(final Memory mem) {
    return mem.getByte(M_BYTE_ADR) & 0XFF;
  }

  static long getMemoryN(final Memory mem) {
    return mem.getLong(N_LONG_ADR);
  }

  static int getMemoryMinK(final Memory mem) {
    return mem.getShort(MIN_K_SHORT_ADR) & 0XFFFF;
  }

  static int getMemoryNumLevels(final Memory mem) {
    return mem.getByte(NUM_LEVELS_BYTE_ADR) & 0XFF;
  }

*/
