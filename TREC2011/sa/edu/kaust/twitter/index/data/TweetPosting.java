/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

package sa.edu.kaust.twitter.index.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TweetPosting implements WritableComparable<TweetPosting> {

	private long tweetID;
	private short tf;

	/**
	 * Creates a new empty posting.
	 */
	public TweetPosting() {
		super();
	}

	/**
	 * Creates a new posting with a specific docno and score.
	 */
	public TweetPosting(long docno, short tf) {
		super();
		this.tweetID = docno;
		this.tf = tf;
	}

	/**
	 * Returns the docno of this posting.
	 */
	public long getTweetID() {
		return tweetID;
	}

	/**
	 * Sets the docno of this posting.
	 */
	public void setTweetID(long id) {
		tweetID = id;
	}

	/**
	 * Returns the score of this posting. Most typically, this is the term
	 * frequency, but can also be the impact score in the case of impact-based
	 * indexes.
	 */
	public short getTF() {
		return tf;
	}

	/**
	 * Sets the score of this posting. Most typically, this is the term
	 * frequency, but can also be the impact score in the case of impact-based
	 * indexes.
	 */
	public void setTF(short tf) {
		this.tf = tf;
	}

	/**
	 * Deserializes this posting.
	 */
	public void readFields(DataInput in) throws IOException {
		tweetID = in.readLong();
		tf = in.readShort();
	}

	/**
	 * Serializes this posting.
	 */
	public void write(DataOutput out) throws IOException {
		out.writeLong(tweetID);
		out.writeShort(tf);
	}

	/**
	 * Clones this posting.
	 */
	public TweetPosting clone() {
		return new TweetPosting(this.getTweetID(), this.getTF());
	}

	/**
	 * Generates a human-readable String representation of this object.
	 */
	public String toString() {
		return "(" + tweetID + ", " + tf + ")";
	}

	@Override
	public int compareTo(TweetPosting tp) {
		if(tweetID > tp.tweetID) return -1;
		else if(tweetID < tp.tweetID) return 1;
		else return 0;
	}
}
