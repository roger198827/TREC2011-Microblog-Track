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

public class IDFScoringFunction extends ScoringFunction {
	
	float idf;
	
	@Override
	public void initialize(GlobalTermEvidence termEvidence, GlobalEvidence globalEvidence) {
		idf = (float) Math.log((float) globalEvidence.numDocs / (float) termEvidence.getDf());	
		//idf = (float) Math.log((globalEvidence.numDocs - termEvidence.getDf() + 0.5f) / (termEvidence.getDf() + 0.5f));
	}
	@Override
	public float getScore(int tf, int docLen) {
		return idf;
	}
	
	@Override
	public float getMinScore() {
		return 0.0f;
	}
}
