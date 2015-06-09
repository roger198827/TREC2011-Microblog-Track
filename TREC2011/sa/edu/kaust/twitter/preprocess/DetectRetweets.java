/* * Ivory: A Hadoop toolkit for Web-scale information retrieval
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

package sa.edu.kaust.twitter.preprocess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sa.edu.kaust.twitter.data.Tweet;
import sa.edu.kaust.twitter.data.TweetWritable;
import sa.edu.kaust.twitter.tokenize.TweetToken;
import sa.edu.kaust.twitter.tokenize.TweetTokenizer;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;

import com.twitter.corpus.data.HtmlStatus;
import com.twitter.corpus.data.Status;

import edu.umd.cloud9.io.pair.PairOfLongString;

public class DetectRetweets{
	private static final Logger sLogger = Logger.getLogger(DetectRetweets.class);

	protected static enum Tweets {
		MAP_TOTAL_TWEETS, MAP_DETECTED_RETWEETS, REDUCE_DETECTED_ORIGINAL_RETWEETED, 
	}

	private static class MyMapper extends Mapper<LongWritable, TweetWritable, IntWritable, LongWritable> {

		private LongWritable outValue = new LongWritable();
		private IntWritable outKey = new IntWritable();
		public static long startID;
		public static long endID;

		public void setup(Mapper<LongWritable, TweetWritable, IntWritable, LongWritable>.Context context) {
			startID= Long.parseLong(context.getConfiguration().get("startID"));
			endID= Long.parseLong(context.getConfiguration().get("endID"));
		}

		ArrayList<TweetToken> tokens = null;
		public void map(LongWritable key, TweetWritable value, Context context) throws IOException, InterruptedException {
			Tweet tweet = (Tweet)value;

			if (tweet.getID()<startID || tweet.getID()>endID) return;
			Text reTweetContent = new Text();
			context.getCounter(Tweets.MAP_TOTAL_TWEETS).increment(1);
			tokens = TweetTokenizer.getTokenStream(tweet.getMessage()); // tokenize the tweet
			outValue.set(tweet.getID());
			if(TweetTokenizer.detectRetweet(tweet, reTweetContent, tokens)){ 
				// it's a retweet
				outKey.set(reTweetContent.toString().hashCode());
				context.getCounter(Tweets.MAP_DETECTED_RETWEETS).increment(1);
			}
			else{ // not a retweet
				outKey.set(tweet.getMessage().hashCode());
			}
			context.write(outKey, outValue);
		}
	}

	private static class MyReducer extends Reducer<IntWritable, LongWritable, Text, Text> {
		private Text outValue = new Text();
		private Text outKey = new Text();

		public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			Iterator<LongWritable> it = values.iterator();
			ArrayList<Long> a = new ArrayList<Long>();
			while(it.hasNext()){
				a.add(new Long(it.next().get()));
			}
			if(a.size()>1){
				Collections.sort(a);
				long origTweetID = a.get(0).longValue();
				outKey.set(origTweetID+"");
				for(int i = 1; i<a.size(); i++){
					outValue.set(a.get(i).longValue()+"");
					context.write(outKey, outValue);
				}
				context.getCounter(Tweets.REDUCE_DETECTED_ORIGINAL_RETWEETED).increment(1);
			}
		}
	}

	private static int printUsage() {
		System.out.println("usage: [input-dir] [output-dir] [num-of-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@SuppressWarnings("unused")
	public static void runDetectRetweet(String input, String output, String startID, String endID) throws Exception {

		/*if (args.length != 3) {
			printUsage();
			return;
		}

		String input = args[0];
		String output = args[1];
		int reduceTasks = Integer.parseInt(args[2]);*/

		//Path inputPath = new Path("/shared/tweets2011");
		//Path outputPath = new Path("/user/telsayed/tweets2011");

		Path inputPath = new Path(input);
		Path outputPath = new Path(output);

		sLogger.info("input dir: " + inputPath);
		sLogger.info("output dir: " + outputPath);
		//sLogger.info("num of output files: " + reduceTasks);

		int mapTasks = 100;

		Configuration conf = new Configuration();
		conf.set("startID", startID);
		conf.set("endID", endID);
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "DetectRetweets");
		job.setJarByClass(DetectRetweets.class);

		/*if (fs.exists(outputPath)) {
			sLogger.info("Output path already exist: skipping!");
			return;
		}*/

		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//conf.set("mapred.child.java.opts", "-Xmx2048m");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(output), true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		fs.copyToLocalFile(new Path(output+"/part-r-00000"), new Path("retweet/part-r-00000"));
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0	+ " seconds");
	}

	public static void main(String[] args) throws Exception
	{
		runDetectRetweet(args[0], args[1], args[2], args[3]); 
	}
}
