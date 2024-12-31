package cpc

import "fmt"

type CpcCompressedState struct {
	CsvIsValid    bool
	WindowIsValid bool
	LgK           int
	SeedHash      int16
	FiCol         int
	MergeFlag     bool // compliment of HIP Flag
	NumCoupons    int64

	Kxp         float64
	HipEstAccum float64

	NumCsv        int
	CsvStream     []int // may be longer than required
	CsvLengthInts int
	CwStream      []int // may be longer than required
	CwLengthInts  int
}

func NewCpcCompressedState(lgK int, seedHash int16) *CpcCompressedState {
	return &CpcCompressedState{
		LgK:      lgK,
		SeedHash: seedHash,
		Kxp:      float64(int(1) << lgK),
	}
}

func importFromMemory(bytes []byte) (*CpcCompressedState, error) {
	if err := checkLoPreamble(bytes); err != nil {
		return nil, err
	}
	if !isCompressed(bytes) {
		return nil, fmt.Errorf("not compressed")
	}
	lgK := getLgK(bytes)
	seedHash := getSeedHash(bytes)
	state := NewCpcCompressedState(lgK, seedHash)
	fmtOrd := getFormatOrdinal(bytes)
	format := CpcFormat(fmtOrd)
	state.MergeFlag = (fmtOrd & 1) == 0
	state.CsvIsValid = (fmtOrd & 2) > 0
	state.WindowIsValid = (fmtOrd & 4) > 0

	switch format {
	default:
		panic("not implemented")
	}
}

/*
static CompressedState importFromMemory(final Memory mem) {
    checkLoPreamble(mem);
    rtAssert(isCompressed(mem));
    final int lgK = getLgK(mem);
    final short seedHash = getSeedHash(mem);
    final CompressedState state = new CompressedState(lgK, seedHash);
    final int fmtOrd = getFormatOrdinal(mem);
    final Format format = Format.ordinalToFormat(fmtOrd);
    state.mergeFlag = !((fmtOrd & 1) > 0); //merge flag is complement of HIP
    state.csvIsValid = (fmtOrd & 2) > 0;
    state.windowIsValid = (fmtOrd & 4) > 0;

    switch (format) {
      case EMPTY_MERGED :
      case EMPTY_HIP : {
        checkCapacity(mem.getCapacity(), 8L);
        break;
      }
      case SPARSE_HYBRID_MERGED : {
        //state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = (int) state.numCoupons; //only true for sparse_hybrid
        state.csvLengthInts = getSvLengthInts(mem);
        //state.cwLength = getCwLength(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        //state.cwStream = getCwStream(mem);
        state.csvStream = getSvStream(mem);
        break;
      }
      case SPARSE_HYBRID_HIP : {
        //state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = (int) state.numCoupons; //only true for sparse_hybrid
        state.csvLengthInts = getSvLengthInts(mem);
        //state.cwLength = getCwLength(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        //state.cwStream = getCwStream(mem);
        state.csvStream = getSvStream(mem);
        break;
      }
      case PINNED_SLIDING_MERGED_NOSV : {
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        //state.numCsv = getNumCsv(mem);
        //state.csvLength = getCsvLength(mem);
        state.cwLengthInts = getWLengthInts(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(mem);
        //state.csvStream = getCsvStream(mem);
        break;
      }
      case PINNED_SLIDING_HIP_NOSV : {
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        //state.numCsv = getNumCsv(mem);
        //state.csvLength = getCsvLength(mem);
        state.cwLengthInts = getWLengthInts(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(mem);
        //state.csvStream = getCsvStream(mem);
        break;
      }
      case PINNED_SLIDING_MERGED : {
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = getNumSv(mem);
        state.csvLengthInts = getSvLengthInts(mem);
        state.cwLengthInts = getWLengthInts(mem);
        //state.kxp = getKxP(mem);
        //state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(mem);
        state.csvStream = getSvStream(mem);
        break;
      }
      case PINNED_SLIDING_HIP : {
        state.fiCol = getFiCol(mem);
        state.numCoupons = getNumCoupons(mem);
        state.numCsv = getNumSv(mem);
        state.csvLengthInts = getSvLengthInts(mem);
        state.cwLengthInts = getWLengthInts(mem);
        state.kxp = getKxP(mem);
        state.hipEstAccum = getHipAccum(mem);
        checkCapacity(mem.getCapacity(), state.getRequiredSerializedBytes());
        state.cwStream = getWStream(mem);
        state.csvStream = getSvStream(mem);
        break;
      }
    }
    checkCapacity(mem.getCapacity(),
        4L * (getPreInts(mem) + state.csvLengthInts + state.cwLengthInts));
    return state;
  }
*/
