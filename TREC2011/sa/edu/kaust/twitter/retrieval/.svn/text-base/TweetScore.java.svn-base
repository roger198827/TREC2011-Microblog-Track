/*
 * Cloud9: A MapReduce Library for Hadoop
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

package sa.edu.kaust.twitter.retrieval;

public class TweetScore implements Comparable<TweetScore> {
	public float score;
	public long tweetID;
	
	public float contentSimScore, recencyScore, userPopularityScore, tweetPopularityScore, userTopicAuthorityScore, urlPopularityScore;

	/**
	 * Creates a pair.
	 */
	public TweetScore() {}

	/**
	 * Creates a pair.
	 *
	 * @param left the left element
	 * @param right the right element
	 */
	public TweetScore(float sc, long tID) {
		score = sc;
		tweetID = tID;
	}

	public boolean equals(TweetScore ts) {
		return score == ts.score && tweetID == ts.tweetID;
	}

	/**
	 * Defines a natural sort order for pairs. Pairs are sorted first by the
	 * left element, and then by the right element.
	 *
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this pair should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	public int compareTo(TweetScore ts) {
		float pl = ts.score;
		long pr = ts.tweetID;

		if (score == pl) {
			if (tweetID < pr)
				return 1;
			if (tweetID > pr)
				return -1;
			return 0;
		}

		if (score < pl)
			return -1;

		return 1;
	}

	/**
	 * Returns a hash code value for the pair.
	 *
	 * @return hash code for the pair
	 */
	public int hashCode() {
		return (int) (score + tweetID);
		//+ (int) contentSimScore + (int) recencyScore + (int) userPopularityScore 
		//+ (int) tweetPopularityScore + (int) urlPopularityScore + (int) userTopicAuthorityScore;
	}

	/**
	 * Generates human-readable String representation of this pair.
	 *
	 * @return human-readable String representation of this pair
	 */
	public String toString() {
		//return "(" + score + ", " + tweetID + ")";
		return "\t(" + score + ", " + tweetID + ")["+"cnt:"+contentSimScore+" time:"+recencyScore+" url:"+
				urlPopularityScore+" ret:"+tweetPopularityScore+" usrpop:"+userPopularityScore + " usrtpc:"+userTopicAuthorityScore+"]";
	}

}
