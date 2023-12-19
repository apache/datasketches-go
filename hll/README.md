

# HllSketch

HllSketch is actually a collection of compact implementations of Phillipe Flajolet’s HyperLogLog (HLL) sketch but with significantly improved error behavior and excellent speed performance.

If the use case for sketching is primarily counting uniques and merging, the HLL sketch is the 2nd highest
performing in terms of accuracy for storage space consumed in the DataSketches library (the new CPC sketch developed by Kevin J. Lang now beats HLL in terms of accuracy / space).


For large counts, HLL sketches can be 2 to 8 times smaller for the same accuracy than the DataSketches Theta Sketches when serialized, but the Theta sketches can do set intersections and differences while HLL and CPC cannot.

The CPC sketch and HLL share similar use cases, but the CPC sketch is about 30 to 40% smaller than the HLL sketch when serialized and larger than the HLL when active in memory.  Choose your weapons!


A new HLL sketch is created with a simple constructor:

    lgK := 12 // This is log-base2 of k, so k = 4096. lgK can be from 4 to 21
    sketch := NewHllSketch(lgK) // TgtHllType_HLL_4 is the default
    // OR
    sketch := NewHllSketch(lgK, TgtHllType_HLL_6)
    // OR
    sketch := NewHllSketch(lgK, TgtHllType_HLL_8)


All three different sketch types are targets in that the sketches start out in a warm-up mode that is small in size and gradually grows as needed until the full HLL array is allocated. 

The HLL_4, HLL_6 and HLL_8 represent different levels of compression of the final HLL array where the 4, 6 and 8 refer to the number of bits each bucket of the HLL array is compressed down to.

The HLL_4 is the most compressed but generally slower than the other two, especially during union operations.</p>

All three types share the same API. Updating the HllSketch is very simple:

	for i := 0; i < 1000000; i++ {
	  sketch.UpdateInt64(i);
	}

Each of the presented integers above are first hashed into 128-bit hash values that are used by the sketch
HLL algorithm, so the above loop is essentially equivalent to using a random number generator initialized with a
seed so that the sequence is deterministic and random.

Obtaining the cardinality results from the sketch is also simple:

	estimate := sketch.GetEstimate()

Note, this is a port from the Java version of the HLL sketch of Apache Datasketches, so the API is slightly different from the Java version.
