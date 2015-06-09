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

package sa.edu.kaust.twitter.index;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import sa.edu.kaust.twitter.data.TweetWritable;
import sa.edu.kaust.twitter.index.data.TweetPostingsList;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.util.map.HMapKL;



/**
 * Object providing an index into one or more <code>SequenceFile</code>s
 * containing {@link IntDocVector}s, providing random access to the document
 * vectors.
 * 
 * @see BuildPostingsForwardIndex
 * 
 * @author Jimmy Lin
 */
public class TweetsForwardIndex {

	private static final Logger sLogger = Logger.getLogger(TweetsForwardIndex.class);
	{
		sLogger.setLevel (Level.WARN);
	}

	private static final NumberFormat sFormatW5 = new DecimalFormat("00000");

	private FileSystem mFs;
	private Configuration mConf;

	//private long[] mPositions;
	private HMapKL<Long> map = null;

	private String mOrigIndexPath;

	private int mCount;

	/**
	 * Creates an <code>IntDocVectorsIndex</code> object.
	 * 
	 * @param indexPath
	 *            location of the index file
	 * @param fs
	 *            handle to the FileSystem
	 * @throws IOException 
	 * @throws IOException
	 */
	public TweetsForwardIndex(String origIndexPath, String fwindexPath, FileSystem fs) throws IOException{
		mFs = fs;
		mConf = fs.getConf();

		mOrigIndexPath = origIndexPath;
		sLogger.debug ("mPath: " + mOrigIndexPath);

		String forwardIndexPath = fwindexPath;
		sLogger.debug ("forwardIndexPath: " + forwardIndexPath);
		FSDataInputStream posInput = fs.open (new Path (forwardIndexPath));

		//mCount = posInput.readInt();

		//mPositions = new long[mCount];
		map= new HMapKL<Long>();
		long tweetID;
		long pos;
		int  i = 0;
		System.out.println("Loading tweets forward index ...");
		while(true){
			try {
				tweetID = posInput.readLong();
				pos = posInput.readLong();
				map.put(tweetID, pos);
			} catch (IOException e) {
				break;
			}
			i++;
			if(i % 1000000 == 0){
			//if(i % 10 == 0){
				//System.out.println(tweetID+"\t"+pos);
				System.out.println("loaded "+ i + " entries ...");
			}
		}
		System.out.println("done ("+i+" entries).");
	}

	/**
	 * Returns the document vector given a docno.
	 */
	public TweetWritable getValue(long tweetID) throws IOException {
		//System.out.println("tweet-id:  "+tweetID);
		long pos;
		try{
			pos = map.get(tweetID);
		}catch(NoSuchElementException e){
			return null;
		}
		if(pos == 0) return null;
		//System.out.println("pos: "+pos);

		int fileNo = (int) (pos / BuildPostingsForwardIndex.BigNumber);
		pos = pos % BuildPostingsForwardIndex.BigNumber;
		//System.out.println(fileNo+"\t"+pos);
		SequenceFile.Reader reader = new SequenceFile.Reader(mFs, new Path(mOrigIndexPath + "/part-"
				+ sFormatW5.format(fileNo)), mConf);

		LongWritable key = new LongWritable();
		TweetWritable value = new TweetWritable();

		/*try {
			value = (ArrayListWritable<IntWritable>) reader.getValueClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate key/value pair!");
		}*/

		reader.seek(pos);
		reader.next(key, value);

		if (key.get() !=tweetID) {
			sLogger.error("unable to find postings for term " + tweetID + ": found term " + key
					+ " instead");
			return null;
		}

		reader.close();
		return value;
	}

	/**
	 * Simple test program.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: [postings-path] [fwindex-path");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		TweetsForwardIndex index = new TweetsForwardIndex(args[0], args[1], FileSystem.get(conf));

		String tweetID = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Look up postings of tweet-id > ");
		while ((tweetID = stdin.readLine()) != null) {
			TweetWritable pl = index.getValue(Long.parseLong(tweetID));
			if(pl == null)
				System.out.println(tweetID + " not found!");
			else System.out.println(tweetID + ": " + index.getValue(Long.parseLong(tweetID)));
			System.out.print("Look up postings of tweet-id > ");
		}
	}
}
