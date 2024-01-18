package kll

import (
	"encoding/binary"
	"fmt"
	"github.com/apache/datasketches-go/internal"
)

type ItemsSketchMemoryValidate[C comparable] struct {
	srcMem          []byte
	itemSketchOp    ItemSketchOp[C]
	sketchStructure sketchStructure

	// first 8 bytes of preamble
	preInts  int //used by KllPreambleUtil
	serVer   int //used by KllPreambleUtil
	familyID int //used by KllPreambleUtil
	flags    int //used by KllPreambleUtil
	k        int //used multiple places
	m        int //used multiple places
	//byte 7 is unused

	//Flag bits:
	emptyFlag        bool //used multiple places
	level0SortedFlag bool //used multiple places

	// depending on the layout, the next 8-16 bytes of the preamble, may be derived by assumption.
	// For example, if the layout is compact & empty, n = 0, if compact and single, n = 1.
	n         int64 //8 bytes (if present), used multiple places
	minK      int   //2 bytes (if present), used multiple places
	numLevels int   //1 byte  (if present), used by KllPreambleUtil
	//skip unused byte
	levelsArr []int //starts at byte 20, adjusted to include top index here, used multiple places

	// derived.
	sketchBytes int //used by KllPreambleUtil
	typeBytes   int //always 0 for generic
}

func NewItemsSketchMemoryValidate[C comparable](srcMem []byte, itemSketchOp ItemSketchOp[C]) (*ItemsSketchMemoryValidate[C], error) {
	capa := cap(srcMem)
	if capa < 8 {
		return nil, fmt.Errorf("Memory too small: %d", capa)
	}
	preInts := getPreInts(srcMem)
	serVer := getSerVer(srcMem)
	sketchStructure := getSketchStructure(preInts, serVer)
	familyID := getFamilyID(srcMem)
	if familyID != internal.FamilyEnum.Kll.Id {
		return nil, fmt.Errorf("Source not KLL: %d", familyID)
	}
	flags := getFlags(srcMem)
	k := getK(srcMem)
	m := getM(srcMem)
	err := checkM(m)
	if err != nil {
		return nil, err
	}
	err = checkK(k, m)
	if err != nil {
		return nil, err
	}
	//flags
	emptyFlag := getEmptyFlag(srcMem)
	level0SortedFlag := getLevelZeroSortedFlag(srcMem)
	typeBytes := 0
	vlid := &ItemsSketchMemoryValidate[C]{
		srcMem:           srcMem,
		itemSketchOp:     itemSketchOp,
		sketchStructure:  sketchStructure,
		preInts:          preInts,
		serVer:           serVer,
		familyID:         familyID,
		flags:            flags,
		k:                k,
		m:                m,
		emptyFlag:        emptyFlag,
		level0SortedFlag: level0SortedFlag,
		typeBytes:        typeBytes,
	}
	err = vlid.validate()
	return vlid, err
}

func (vlid *ItemsSketchMemoryValidate[C]) validate() error {
	switch vlid.sketchStructure {
	case _COMPACT_FULL:
		if vlid.emptyFlag {
			return fmt.Errorf("Empty flag and compact full")
		}
		vlid.n = getN(vlid.srcMem)
		vlid.minK = getMinK(vlid.srcMem)
		vlid.numLevels = getNumLevels(vlid.srcMem)
		// Get Levels Arr and add the last element
		vlid.levelsArr = make([]int, vlid.numLevels+1)
		for i := 0; i < vlid.numLevels; i++ {
			vlid.levelsArr[i] = int(binary.LittleEndian.Uint32(vlid.srcMem[_DATA_START_ADR+i*4 : _DATA_START_ADR+i*4+4]))
		}
		capacityItems := computeTotalItemCapacity(uint16(vlid.k), uint8(vlid.m), uint8(vlid.numLevels))
		vlid.levelsArr[vlid.numLevels] = int(capacityItems) //load the last one
		sb, err := computeSketchBytes(vlid.srcMem, vlid.levelsArr, vlid.typeBytes, vlid.itemSketchOp)
		if err != nil {
			return err
		}
		vlid.sketchBytes = sb

	case _COMPACT_EMPTY:
		if !vlid.emptyFlag {
			return fmt.Errorf("Empty flag and compact empty")
		}
		vlid.n = 0 //assumed
		vlid.minK = vlid.k
		vlid.numLevels = 1 //assumed
		vlid.levelsArr = []int{vlid.k, vlid.k}
		vlid.sketchBytes = _DATA_START_ADR_SINGLE_ITEM
	case _COMPACT_SINGLE:
		if vlid.emptyFlag {
			return fmt.Errorf("Empty flag and compact single")
		}
		vlid.n = 1 //assumed
		vlid.minK = vlid.k
		vlid.numLevels = 1 //assumed
		vlid.levelsArr = []int{vlid.k - 1, vlid.k}
		v, err := vlid.itemSketchOp.sizeOfMany(vlid.srcMem, _DATA_START_ADR_SINGLE_ITEM, 1)
		if err != nil {
			return err
		}
		vlid.sketchBytes = _DATA_START_ADR_SINGLE_ITEM + v
	default:
		return fmt.Errorf("Invalid preamble ints and serial version combo")
	}
	return nil
}

func computeSketchBytes[C comparable](srcMem []byte, levelsArr []int, typeBytes int, itemSketchOp ItemSketchOp[C]) (int, error) {
	numLevels := len(levelsArr) - 1
	retainedItems := levelsArr[numLevels] - levelsArr[0]
	levelsLen := len(levelsArr) - 1
	numItems := retainedItems
	offsetBytes := _DATA_START_ADR + levelsLen*4
	if typeBytes == 1 {
		v, err := itemSketchOp.sizeOfMany(srcMem, offsetBytes, numItems)
		if err != nil {
			return 0, err
		}
		offsetBytes += v + 2 //2 for min & max
	} else {
		v, err := itemSketchOp.sizeOfMany(srcMem, offsetBytes, numItems+2) //2 for min & max
		if err != nil {
			return 0, err
		}
		offsetBytes += v
	}
	return offsetBytes, nil
}
