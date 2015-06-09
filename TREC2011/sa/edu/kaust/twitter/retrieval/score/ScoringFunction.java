/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package sa.edu.kaust.twitter.retrieval.score;

/**
 * Abstract base class of all scoring functions.
 *
 * @author Don Metzler
 *
 */
public abstract class ScoringFunction {

	/**
	 * Initializes this scoring function with global evidence.
	 */
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {}

	/**
	 * Computes score.
	 */
	public abstract float getScore(int tf, int docLen);

	/**
	 * Returns the minimum possible score.
	 */
	public float getMinScore() {
		return Float.NEGATIVE_INFINITY;
	}
	
	/**
	 * Returns the maximum possible score.
	 */
	public float getMaxScore() {
		return Float.POSITIVE_INFINITY;
	}
}
