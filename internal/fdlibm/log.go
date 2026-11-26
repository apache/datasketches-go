// Ported from __ieee754_log in FDLIBM 5.3, the library Java's StrictMath is specified to use.
//
// ====================================================
// Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//
// Developed at SunSoft, a Sun Microsystems, Inc. business.
// Permission to use, copy, modify, and distribute this
// software is freely granted, provided that this notice
// is preserved.
// ====================================================

// Package fdlibm provides FDLIBM functions that return the same bits on every platform,
// matching Java's StrictMath and datasketches-cpp.
package fdlibm

import "math"

const (
	ln2Hi = 6.93147180369123816490e-01 /* 3fe62e42 fee00000 */
	ln2Lo = 1.90821492927058770002e-10 /* 3dea39ef 35793c76 */
	two54 = 1.80143985094819840000e+16 /* 43500000 00000000 */
	lg1   = 6.666666666666735130e-01   /* 3FE55555 55555593 */
	lg2   = 3.999999999940941908e-01   /* 3FD99999 9997FA04 */
	lg3   = 2.857142874366239149e-01   /* 3FD24924 94229359 */
	lg4   = 2.222219843214978396e-01   /* 3FCC71C5 1D8E78AF */
	lg5   = 1.818357216161805012e-01   /* 3FC74664 96CB03DE */
	lg6   = 1.531383769920937332e-01   /* 3FC39A09 D078C69F */
	lg7   = 1.479819860511658591e-01   /* 3FC2F112 DF3E5244 */
)

// Log returns the natural logarithm of x.
func Log(x float64) float64 {
	hx := int32(math.Float64bits(x) >> 32)
	lx := uint32(math.Float64bits(x))

	k := int32(0)
	if hx < 0x00100000 { // x < 2**-1022
		if (uint32(hx)&0x7fffffff)|lx == 0 {
			return math.Inf(-1)
		}
		if hx < 0 {
			return math.NaN()
		}
		k -= 54
		x *= two54 // subnormal: scale up
		hx = int32(math.Float64bits(x) >> 32)
	}
	if hx >= 0x7ff00000 {
		return x + x
	}
	k += (hx >> 20) - 1023
	hx &= 0x000fffff
	i := (hx + 0x95f64) & 0x100000
	x = math.Float64frombits(uint64(uint32(hx|(i^0x3ff00000)))<<32 | math.Float64bits(x)&0xffffffff) // normalize x or x/2
	k += i >> 20
	f := x - 1.0
	if (0x000fffff & (2 + hx)) < 3 { // |f| < 2**-20
		if f == 0 {
			if k == 0 {
				return 0
			}
			dk := float64(k)
			return float64(dk*ln2Hi) + float64(dk*ln2Lo)
		}
		r := float64(float64(f*f) * float64(0.5-float64(0.33333333333333333*f)))
		if k == 0 {
			return f - r
		}
		dk := float64(k)
		return float64(dk*ln2Hi) - (float64(r-float64(dk*ln2Lo)) - f)
	}
	s := f / (2.0 + f)
	dk := float64(k)
	z := s * s
	i = hx - 0x6147a
	w := z * z
	j := 0x6b851 - hx
	t1 := float64(w * float64(lg2+float64(w*float64(lg4+float64(w*lg6)))))
	t2 := float64(z * float64(lg1+float64(w*float64(lg3+float64(w*float64(lg5+float64(w*lg7)))))))
	i |= j
	r := t2 + t1
	if i > 0 {
		hfsq := float64(float64(0.5*f) * f)
		if k == 0 {
			return f - float64(hfsq-float64(s*(hfsq+r)))
		}
		return float64(dk*ln2Hi) - (float64(hfsq-float64(float64(s*(hfsq+r))+float64(dk*ln2Lo))) - f)
	}
	if k == 0 {
		return f - float64(s*(f-r))
	}
	return float64(dk*ln2Hi) - (float64(float64(s*(f-r))-float64(dk*ln2Lo)) - f)
}
