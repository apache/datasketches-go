/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tuple

import (
	"errors"
)

// lowerBoundForBOverAInSketchedSets gets the approximate lower bound for B over A
// based on a 95% confidence interval
func lowerBoundForBOverAInSketchedSets[S Summary](a, b Sketch[S]) (float64, error) {
	theta64A := a.Theta64()
	theta64B := b.Theta64()

	if err := validateThetas(theta64A, theta64B); err != nil {
		return 0, err
	}

	countB := uint64(b.NumRetained())
	var countA uint64
	if theta64A == theta64B {
		countA = uint64(a.NumRetained())
	} else {
		countA = countLessThanTheta64(a, theta64B)
	}

	if countA == 0 {
		return 0, nil
	}

	f := b.Theta()
	result, err := lowerBoundForBOverA(countA, countB, f)
	if err != nil {
		return 0, err
	}
	return result, nil
}

// upperBoundForBOverAInSketchedSets returns the approximate upper bound for B over A
// based on a 95% confidence interval
func upperBoundForBOverAInSketchedSets[S Summary](a, b Sketch[S]) (float64, error) {
	theta64A := a.Theta64()
	theta64B := b.Theta64()

	if err := validateThetas(theta64A, theta64B); err != nil {
		return 0, err
	}

	countB := uint64(b.NumRetained())
	var countA uint64
	if theta64A == theta64B {
		countA = uint64(a.NumRetained())
	} else {
		countA = countLessThanTheta64(a, theta64B)
	}

	if countA == 0 {
		return 1, nil
	}

	f := b.Theta()
	result, err := upperBoundForBOverA(countA, countB, f)
	if err != nil {
		return 0, err
	}
	return result, nil
}

// estimateOfBOverAInSketchedSets returns the estimate for B over A
func estimateOfBOverAInSketchedSets[S Summary](a, b Sketch[S]) (float64, error) {
	theta64A := a.Theta64()
	theta64B := b.Theta64()

	if err := validateThetas(theta64A, theta64B); err != nil {
		return 0, err
	}

	countB := uint64(b.NumRetained())
	var countA uint64
	if theta64A == theta64B {
		countA = uint64(a.NumRetained())
	} else {
		countA = countLessThanTheta64(a, theta64B)
	}

	if countA == 0 {
		return 0.5, nil
	}

	return float64(countB) / float64(countA), nil
}

func validateThetas(thetaA, thetaB uint64) error {
	if thetaB > thetaA {
		return errors.New("theta_a must be <= theta_b")
	}
	return nil
}

func countLessThanTheta64[S Summary](sketch Sketch[S], theta uint64) uint64 {
	count := uint64(0)
	for hash := range sketch.All() {
		if hash < theta {
			count++
		}
	}
	return count
}
