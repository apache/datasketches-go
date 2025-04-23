package count

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"

	"github.com/apache/datasketches-go/internal"
)

type countMinSketch struct {
	numBuckets    int32 // counter array for each of the hashing function
	numHashes     int8  // number of hashing functions
	sketchSlice   []int64
	seed          int64
	totatlWeights int64
	hashSeeds     []int64
}

func NewCountMinSketch(numHashes int8, numBuckets int32, seed int64) (*countMinSketch, error) {
	if numBuckets < 3 {
		return nil, errors.New("using fewer than 3 buckets incurs relative error greater than 1.0")
	}

	if numBuckets*int32(numHashes) >= 1<<30 {
		return nil, errors.New("these parameters generate a sketch that exceeds 2^30 elements")
	}

	rng := rand.New(rand.NewSource(seed))
	hashSeeds := make([]int64, numHashes)
	for i := range int(numHashes) {
		hashSeeds[i] = int64(rng.Int()) + seed
	}

	sketchSize := int(numBuckets * int32(numHashes))
	sketchSlice := make([]int64, sketchSize)

	return &countMinSketch{
		numBuckets:  numBuckets,
		numHashes:   numHashes,
		sketchSlice: sketchSlice,
		seed:        seed,
		hashSeeds:   hashSeeds,
	}, nil
}

func (c *countMinSketch) getNumBuckets() int32 {
	return c.numBuckets
}

func (c *countMinSketch) getNumHashes() int8 {
	return c.numHashes
}

func (c *countMinSketch) getTotalWeights() int64 {
	return c.totatlWeights
}

func (c *countMinSketch) getSeed() int64 {
	return c.seed
}

func (c *countMinSketch) getRelativeError() float64 {
	return math.Exp(1.0) / float64(c.numBuckets)
}

func (c *countMinSketch) isEmpty() bool {
	return c.totatlWeights == 0
}

func (c *countMinSketch) getHashes(item []byte) []int64 {
	var bucketIndex, hashSeedIndex uint64
	sketchUpdateLocations := make([]int64, c.numHashes)

	for i, s := range c.hashSeeds {
		h1, _ := internal.HashByteArrMurmur3(item, 0, len(item), uint64(s))
		bucketIndex = h1 % uint64(c.numBuckets)
		sketchUpdateLocations[i] = int64(hashSeedIndex)*int64(c.numBuckets) + int64(bucketIndex)
		hashSeedIndex++
	}

	return sketchUpdateLocations
}

func (c *countMinSketch) Update(item []byte, weight int64) error {
	if len(item) == 0 {
		return nil
	}

	if weight < 0 {
		c.totatlWeights += -weight
	} else {
		c.totatlWeights += weight
	}

	hashLocations := c.getHashes(item)
	for _, h := range hashLocations {
		c.sketchSlice[h] += weight
	}
	return nil
}

func (c *countMinSketch) UpdateUint64(item uint64, weight int64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, item)
	return c.Update(b, weight)
}

func (c *countMinSketch) UpdateString(item string, weight int64) error {
	if len(item) == 0 {
		return nil
	}

	return c.Update([]byte(item), weight)
}

func (c *countMinSketch) GetEstimate(item []byte) int64 {
	if len(item) == 0 {
		return 0
	}

	hashLocations := c.getHashes(item)
	estimate := int64(math.MaxInt64)
	for _, h := range hashLocations {
		estimate = Min(estimate, c.sketchSlice[h])
	}
	return estimate
}

func (c *countMinSketch) GetEstimateUint64(item uint64) int64 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, item)
	return c.GetEstimate(b)
}

func (c *countMinSketch) GetEstimateString(item string) int64 {
	if len(item) == 0 {
		return 0
	}
	return c.GetEstimate([]byte(item))
}

func (c *countMinSketch) GetUpperBound(item []byte) int64 {
	return c.GetEstimate(item) + int64(c.getRelativeError()*float64(c.getTotalWeights()))
}

func (c *countMinSketch) GetUpperBoundUint64(item uint64) int64 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, item)
	return c.GetUpperBound(b)
}

func (c *countMinSketch) GetLowerBound(item []byte) int64 {
	return c.GetEstimate(item)
}

func (c *countMinSketch) GetLowerBoundUint64(item uint64) int64 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, item)
	return c.GetLowerBound(b)
}

func (c *countMinSketch) Merge(otherSketch *countMinSketch) error {
	if c == otherSketch {
		return errors.New("cannot merge sketch with itself")
	}

	canMerge := c.getNumHashes() == otherSketch.getNumHashes() &&
		c.getNumBuckets() == otherSketch.getNumBuckets() &&
		c.getSeed() == otherSketch.getSeed()

	if !canMerge {
		return errors.New("sketches are incompatible")
	}

	for i := range c.sketchSlice {
		c.sketchSlice[i] += otherSketch.sketchSlice[i]
	}
	c.totatlWeights += otherSketch.totatlWeights

	return nil
}

func (c *countMinSketch) Serialize(w io.Writer) error {
	preambleLongs := byte(PREAMBLE_LONGS_SHORT)
	serVer := byte(SERIAL_VERSION_1)
	familyID := byte(FAMILY_ID)

	var flagsByte byte
	if c.isEmpty() {
		flagsByte |= 1 << IS_EMPTY
	}
	unused32 := uint32(NULL_32)

	if err := binary.Write(w, binary.LittleEndian, preambleLongs); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, serVer); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, familyID); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, flagsByte); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, unused32); err != nil {
		return err
	}

	seedHash, err := internal.ComputeSeedHash(c.seed)
	if err != nil {
		return err
	}
	unused8 := byte(NULL_8)

	if err := binary.Write(w, binary.LittleEndian, c.numBuckets); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, c.numHashes); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, seedHash); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, unused8); err != nil {
		return err
	}

	// Skip rest if sketch is empty
	if c.isEmpty() {
		return nil
	}

	if err := binary.Write(w, binary.LittleEndian, c.totatlWeights); err != nil {
		return err
	}

	for _, h := range c.sketchSlice {
		err := binary.Write(w, binary.LittleEndian, h)
		if err != nil {
			return err
		}
	}

	return nil
}
