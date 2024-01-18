package kll

type sketchStructure struct {
	preInts int
	serVer  int
}

var (
	_COMPACT_EMPTY  = sketchStructure{_PREAMBLE_INTS_EMPTY_SINGLE, _SERIAL_VERSION_EMPTY_FULL}
	_COMPACT_SINGLE = sketchStructure{_PREAMBLE_INTS_EMPTY_SINGLE, _SERIAL_VERSION_SINGLE}
	_COMPACT_FULL   = sketchStructure{_PREAMBLE_INTS_FULL, _SERIAL_VERSION_EMPTY_FULL}
	_UPDATABLE      = sketchStructure{_PREAMBLE_INTS_FULL, _SERIAL_VERSION_UPDATABLE}
)

func (s sketchStructure) getPreInts() int { return s.preInts }

func (s sketchStructure) getSerVer() int { return s.serVer }

func getSketchStructure(preInts, serVer int) sketchStructure {
	if preInts == _PREAMBLE_INTS_EMPTY_SINGLE {
		if serVer == _SERIAL_VERSION_EMPTY_FULL {
			return _COMPACT_EMPTY
		} else if serVer == _SERIAL_VERSION_SINGLE {
			return _COMPACT_SINGLE
		}
	} else if preInts == _PREAMBLE_INTS_FULL {
		if serVer == _SERIAL_VERSION_EMPTY_FULL {
			return _COMPACT_FULL
		} else if serVer == _SERIAL_VERSION_UPDATABLE {
			return _UPDATABLE
		}
	}
	panic("Invalid preamble ints and serial version combo")
}

/*
   public static SketchStructure getSketchStructure(final int preInts, final int serVer) {
     final SketchStructure[] ssArr = SketchStructure.values();
     for (int i = 0; i < ssArr.length; i++) {
       if (ssArr[i].preInts == preInts && ssArr[i].serVer == serVer) {
         return ssArr[i];
       }
     }
     throw new SketchesArgumentException("Error combination of PreInts and SerVer: "
         + "PreInts: " + preInts + ", SerVer: " + serVer);
   }
*/
