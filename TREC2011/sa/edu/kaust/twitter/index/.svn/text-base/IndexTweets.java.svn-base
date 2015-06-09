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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sa.edu.kaust.twitter.data.TweetWritable;
import sa.edu.kaust.twitter.index.data.TweetPosting;
import sa.edu.kaust.twitter.index.data.TweetPostingsList;
import sa.edu.kaust.twitter.preprocess.hashtag.HashtagRepresentation;
import sa.edu.kaust.twitter.preprocess.url.UrlRepresentation;
import sa.edu.kaust.twitter.tokenize.TweetToken;
import sa.edu.kaust.twitter.tokenize.TweetTokenizer;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStringLong;

public class IndexTweets {
	private static final Logger sLogger = Logger.getLogger(IndexTweets.class);

	protected static enum Docs { Total }
	protected static enum IndexedTerms { Unique, Total }
	protected static enum MapTime { Total }
	protected static enum ReduceTime { Total }

	private static class MyMapper extends Mapper<LongWritable, TweetWritable, PairOfStringLong, IntWritable> {
		public static long startID;
		public static long endID;
		private static final IntWritable termTF = new IntWritable();
		private static final PairOfStringLong pair = new PairOfStringLong(); // pair of term and tweet id
		private long tweetID;
		ArrayList<TweetToken> tokens = null;
		Map<String, Short> map = new HashMap<String, Short>();
		Short tf;
		HashtagRepresentation hashtagRepresentation;
		UrlRepresentation urlRepresentation;		
		ArrayListWritable<PairOfStringInt>hashtagRep=new ArrayListWritable<PairOfStringInt>();
		ArrayListWritable<PairOfStringInt>urlRep=new ArrayListWritable<PairOfStringInt>();
		String term;
		boolean urlExpand = true;
		boolean hashtagExpand = true;
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			startID= Long.parseLong(conf.get("startID"));
			endID= Long.parseLong(conf.get("endID"));
			urlExpand = conf.getBoolean("expandURL", true);
			hashtagExpand=conf.getBoolean("expandHashtag", true);
			if(!urlExpand&&!hashtagExpand) return;
			FileSystem fs=FileSystem.get(conf);
			try {
				// Detect if we're in standalone mode; if so, we can't us the
				// DistributedCache because it does not (currently) work in
				// standalone mode...
				if (conf.get("mapred.job.tracker").equals("local")) {
					if(hashtagExpand)
					hashtagRepresentation=new HashtagRepresentation(new Path(conf.get("HashtagRepresentation")),fs);
					if(urlExpand)
					urlRepresentation=new UrlRepresentation(new Path(conf.get("UrlRepresentation")),fs);					 
				} else {					
					Path []cacheFiles=DistributedCache.getLocalCacheFiles(conf);
					if(hashtagExpand)
					hashtagRepresentation=new HashtagRepresentation(cacheFiles[0],FileSystem.getLocal(conf));
					if(urlExpand)
					urlRepresentation=new UrlRepresentation(cacheFiles[1],FileSystem.getLocal(conf));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing HashtagRepresentation and UrlRepresentation!");
			} 
		} 

		

		public void map(LongWritable key, TweetWritable tweet, Context context) throws IOException, InterruptedException {
			tweetID = tweet.getID();			
			if (tweetID<startID || tweetID>endID) return;
			if(tweet.getCode() == 302) return; // don't index retweets
			tokens = TweetTokenizer.getTokenStream(tweet.getMessage()); // tokenize the tweet
			if(TweetTokenizer.isRetweet(0, tokens)) return; // don't index retweets
			map.clear();
			
			if(hashtagRep!=null)
					hashtagRep.clear();				
			if(urlRep!=null)
					urlRep.clear();
			
			for(TweetToken token: tokens){
				if(hashtagExpand){
					if(token.type==TweetTokenType.HASHTAG){ // expand the hashtag
						hashtagRep=hashtagRepresentation.getValue(token.text);
						if(hashtagRep!=null){
							for(PairOfStringInt p : hashtagRep){
								term = p.getLeftElement();
								tf = map.get(term);
								if(tf == null) map.put(term, new Short((short) 1));
								else map.put(term, new Short((short)(tf.shortValue()+1)));
							}
						}
						tf = map.get(token.text);
						if(tf == null) map.put(token.text, new Short((short) 1));
						else map.put(token.text, new Short((short)(tf.shortValue()+1)));
					}
				}
				if(urlExpand){
					 if(token.type==TweetTokenType.URL){ // expand the URL					
						urlRep=urlRepresentation.getValue(token.text.hashCode());
						if(urlRep!=null){
							for(PairOfStringInt p : urlRep){
								term = p.getLeftElement();
								tf = map.get(term);
								if(tf == null) map.put(term, new Short((short) 1));
								else map.put(term, new Short((short)(tf.shortValue()+1)));
							}
						}
					}
				}
				
				if(token.type==TweetTokenType.OTHER||token.type==TweetTokenType.HASHTAG){
					tf = map.get(token.text);
					if(tf == null) map.put(token.text, new Short((short) 1));
					else map.put(token.text, new Short((short)(tf.shortValue()+1)));
				}
			}

			for(Map.Entry<String, Short> e : map.entrySet()){
				pair.set(e.getKey(), -tweetID);
				termTF.set(e.getValue());
				context.write(pair, termTF);
			}
		}
	}

	private static class MyReducer extends Reducer<PairOfStringLong, IntWritable, Text, TweetPostingsList> {
		private static final Text term = new Text();
		private static final TweetPostingsList postings = new TweetPostingsList();
		private String prevTerm = "";
		String curTerm;

		protected void reduce(PairOfStringLong pair, Iterable<IntWritable> values, Context context) throws IOException,
		InterruptedException {
			curTerm = pair.getLeftElement();
			if(!curTerm.equals(prevTerm) && !prevTerm.isEmpty()){
				term.set(prevTerm);
				context.write(term, postings);
				postings.clear();
			}
			postings.add(new TweetPosting(pair.getRightElement(), (short)values.iterator().next().get()));
			prevTerm = curTerm;
		}

		protected void cleanup(Context context) throws IOException,	InterruptedException {
			if(!prevTerm.isEmpty()){
				term.set(prevTerm);
				context.write(term, postings);
			}
		}
	}

	private static class MyPartitioner extends Partitioner<PairOfStringLong, IntWritable> {
		// Keys with the same terms should go to the same reducer.
		public int getPartition(PairOfStringLong key, IntWritable value, int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	public static void run(String input, String output, int reduceTasks,String hashtag,String url,String startID,String endID, boolean expandHashtag,boolean expandURL) throws Exception {

		Path inputPath = new Path(input);
		Path outputPath = new Path(output);

		sLogger.info("input dir: " + inputPath);
		sLogger.info("output dir: " + outputPath);
		sLogger.info("num of output files: " + reduceTasks);

		Configuration conf = new Configuration();
		conf.set("startID", startID);
		conf.set("endID", endID);
		conf.setBoolean("expandHashtag",expandHashtag);
		conf.setBoolean("expandURL", expandURL);
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "IPIndexTweets");
		job.setJarByClass(IndexTweets.class);
		job.setNumReduceTasks(reduceTasks);		
		if (job.getConfiguration().get("mapred.job.tracker").equals("local")) {
			job.getConfiguration().set("HashtagRepresentation", hashtag);
			job.getConfiguration().set("UrlRepresentation", url);			
		} else {
			DistributedCache.addCacheFile(new URI(hashtag), job.getConfiguration());
			DistributedCache.addCacheFile(new URI(url), job.getConfiguration());
		}


		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//conf.set("mapred.child.java.opts", "-Xmx2048m");

		
		job.setInputFormatClass(SequenceFileInputFormat.class);		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);  

		job.setMapOutputKeyClass(PairOfStringLong.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TweetPostingsList.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);

		// delete the output directory if it exists already
		//FileSystem.get(conf).delete(new Path(output), true);
		if (fs.exists(outputPath)) {
			sLogger.info ("Output already exists: skipping!");
			return;
		}

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0	+ " seconds");
	}

	private static int printUsage() {

		System.out.println("usage: [input-dir] [output-dir] [num-of-reducers][hashtag-representation][url-representation][startID][endID][hashtagExpand][urlExpand]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 9) {
			printUsage();
			return;
		}

		indexTweets(args[0], args[1], Integer.parseInt(args[2]),args[3],args[4],args[5],args[6], Boolean.parseBoolean(args[7]),Boolean.parseBoolean(args[8]));	

	}


	public static void indexTweets(String input, String output, int reduceTasks,String hashtag,String url,String startID,String endID, boolean expandHashtag,boolean expandURL) throws Exception
	{
		run(input, output, reduceTasks,hashtag,url,startID,endID, expandHashtag,expandURL);		
	}
}
