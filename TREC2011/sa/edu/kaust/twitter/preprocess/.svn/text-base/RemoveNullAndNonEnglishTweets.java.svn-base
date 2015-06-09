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

package sa.edu.kaust.twitter.preprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.knallgrau.utils.textcat.TextCategorizer;

import sa.edu.kaust.twitter.data.TweetWritable;

import com.twitter.corpus.data.HtmlStatus;
import com.twitter.corpus.data.Status;

import edu.umd.cloud9.io.pair.PairOfLongString;

public class RemoveNullAndNonEnglishTweets{
	private static final Logger sLogger = Logger.getLogger(RemoveNullAndNonEnglishTweets.class);

	protected static enum Tweets {
		TOTAL_TWEETS, NOT_NULL_TWEETS, ENGLISH_CODE_200, ENGLISH_CODE_302, ENGLISH_CODE_403, ENGLISH_CODE_404, NULL_TWEETS, ENGLISH_OTHERS, ENGLISH_TOTAL
	}


	private static class MyMapper extends Mapper<PairOfLongString, HtmlStatus, LongWritable, TweetWritable> {

		TweetWritable tweet = new TweetWritable();
		Status status;
		TextCategorizer guesser = new TextCategorizer();   // Create a TextCategorizer for language detection
		LongWritable outKey = new LongWritable();
		String tweetText = null;
		public void map(PairOfLongString key, HtmlStatus value, Context context) throws IOException, InterruptedException {

			status = Status.fromHtml(key.getLeftElement(), key.getRightElement(),
					value.getHttpStatusCode(), value.getHtml());
			tweet.set(status);


			context.getCounter(Tweets.TOTAL_TWEETS).increment(1);
			if(tweet.isNull()){
				context.getCounter(Tweets.NULL_TWEETS).increment(1);
				return;
			}
			// not null
			context.getCounter(Tweets.NOT_NULL_TWEETS).increment(1);
			
			//----------------------------------------
			// detect if English
			tweetText=tweet.getMessage().toLowerCase();
			tweetText=tweetText.replaceAll("\\(?\\b(http://|www[.])[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]", "");
			if(!guesser.categorize(tweetText).equals("english"))
				return;
			//----------------------------------------
			
			// English
			context.getCounter(Tweets.ENGLISH_TOTAL).increment(1);

			switch (tweet.getCode()){
			case 200:
				context.getCounter(Tweets.ENGLISH_CODE_200).increment(1);
				break;
			case 302:
				context.getCounter(Tweets.ENGLISH_CODE_302).increment(1);
				break;
			case 403:
				context.getCounter(Tweets.ENGLISH_CODE_403).increment(1);
				break;
			case 404:
				context.getCounter(Tweets.ENGLISH_CODE_404).increment(1);
				break;
			default:  context.getCounter(Tweets.ENGLISH_OTHERS).increment(1);
			}
			outKey.set(tweet.getID());
			context.write(outKey, tweet);
		}
	}

	private static class MyReducer extends Reducer<LongWritable, TweetWritable, LongWritable, TweetWritable> {
		public void reduce(LongWritable key, Iterable<TweetWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}

	private static int printUsage() {
		System.out.println("usage: [input-dir] [output-dir] [num-of-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	@SuppressWarnings("unused")

	public static void runRemoveNullAndNonEnglishTweets(String input, String output, int reduceTasks) throws Exception {

		Path inputPath = new Path(input);
		Path outputPath = new Path(output);

		sLogger.info("input dir: " + inputPath);
		sLogger.info("output dir: " + outputPath);
		sLogger.info("num of output files: " + reduceTasks);

		int mapTasks = 100;

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "RemoveNullAndNonEnglishTweets");
		job.setJarByClass(RemoveNullAndNonEnglishTweets.class);

		/*if (fs.exists(outputPath)) {

			sLogger.info("Output path already exist: skipping!");
			return;
		}*/
		fs.delete(outputPath, true);

		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//conf.set("mapred.child.java.opts", "-Xmx2048m");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TweetWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0	+ " seconds");
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			return;
		}
		String input = args[0];
		String output = args[1];
		int reduceTasks = Integer.parseInt(args[2]);
		runRemoveNullAndNonEnglishTweets(input, output, reduceTasks);
	}
}
