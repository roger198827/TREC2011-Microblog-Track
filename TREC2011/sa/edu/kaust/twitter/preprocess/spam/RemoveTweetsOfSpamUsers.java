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

package sa.edu.kaust.twitter.preprocess.spam;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import edu.umd.cloud9.io.*;
import edu.umd.cloud9.io.array.ArrayListWritable;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sa.edu.kaust.twitter.data.TweetWritable;


public class RemoveTweetsOfSpamUsers{

	private static final Logger sLogger = Logger.getLogger(RemoveTweetsOfSpamUsers.class);
	private static Set<String> spamUser=new HashSet<String>();

	protected static enum Statistics {
		NON_SPAM_TWEETS;
	}
	private static class MyMapper extends MapReduceBase implements
	Mapper<LongWritable, TweetWritable, LongWritable, TweetWritable> {
		public static long startID;
		public static long endID;		
		public Boolean spam;
		public void configure(JobConf conf) {
			// load the docid to docno mappings
			try {
				startID= conf.getLong("startID", 0);
				endID= conf.getLong("endID", 0);
				spam = conf.getBoolean("spam", true);
				// Detect if we're in standalone mode; if so, we can't us the
				// DistributedCache because it does not (currently) work in
				// standalone mode...
				if (conf.get("mapred.job.tracker").equals("local")) {
					/*FileSystem fs = FileSystem.get(job);					
					Path p=new Path(job.get("SpamUserListFile"));
					fs.open(p);*/
					FileReader reader = new FileReader(conf.get("SpamUserListFile"));
					BufferedReader br = new BufferedReader(reader);
					String s1=null;					
					while((s1=br.readLine())!=null){
						System.out.println(s1);
						String[]tokens=s1.split("\t");						
						spamUser.add(tokens[0]);						
					}
					br.close();
					reader.close();
				} else {					
					Path []cacheFiles=DistributedCache.getLocalCacheFiles(conf);
					BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
					String s1=null;					
					while((s1=br.readLine())!=null){
						//System.out.println(s1);
						String[]tokens=s1.split("\t");						
						spamUser.add(tokens[0]);						
					}
					br.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing SpamUserListFile!");
			}
		}

		public void map(LongWritable key, TweetWritable value,
				OutputCollector<LongWritable, TweetWritable> output, Reporter reporter) throws IOException {
		
			if(value.getID()<startID || value.getID()>endID) return;
			if (spam)
			{
				if(!spamUser.contains(value.getUserName())){
					output.collect(key, value);
					reporter.incrCounter(Statistics.NON_SPAM_TWEETS, 1);
				}
			}else
			{
				output.collect(key, value);
				reporter.incrCounter(Statistics.NON_SPAM_TWEETS, 1);//non spam tweets will also contain spam
			}
		}
	}
	private static class MyReducer extends MapReduceBase implements
	Reducer<LongWritable, TweetWritable, LongWritable, TweetWritable> {
		public void reduce(LongWritable key, Iterator<TweetWritable> values,
				OutputCollector<LongWritable,TweetWritable> output, Reporter reporter) throws IOException {
			output.collect(key, values.next());
		}
	}



	/**
	 * Creates an instance of this tool.
	 */
	public RemoveTweetsOfSpamUsers() {
	}

	private static int printUsage() {
		System.out.println("usage: [input] [output-dir] [spam user list file] [startID] [endID]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public static int removeTweetsOfSpamUsers(String inputPath, String outputPath, int numReducers, String spamUserListFile,
			long startID, long endID, String nTweetsFile,Boolean spam) throws Exception {
		sLogger.info("input: " + inputPath);
		sLogger.info("output dir: " + outputPath);
		sLogger.info("spam user list file: " + spamUserListFile);

		JobConf conf = new JobConf(RemoveTweetsOfSpamUsers.class);
		FileSystem fs = FileSystem.get(conf);
		conf.setJobName("RemoveSpamUserTweets");
		conf.setLong("startID",startID);
		conf.setLong("endID", endID);
		conf.setNumReduceTasks(numReducers);
		conf.setBoolean("spam", spam);

		// put the mapping file in the distributed cache so each map worker will
		// have it
		//DistributedCache.addCacheFile(new URI(mappingFile), conf);

		if (conf.get("mapred.job.tracker").equals("local")) {
			conf.set("SpamUserListFile", spamUserListFile);
		} else {
			DistributedCache.addCacheFile(new URI(spamUserListFile), conf);
		}

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class); 
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(TweetWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		// delete the output directory if it exists already
		//FileSystem.get(conf).delete(new Path(outputPath), true);
		if (fs.exists(new Path(outputPath))) {
			sLogger.info ("Output already exists: skipping!");
			return FSProperty.readInt(fs, nTweetsFile);
		}

		RunningJob job = JobClient.runJob(conf);
		Counters counters = job.getCounters();
		int nonSpamTweets = (int) counters.findCounter(Statistics.NON_SPAM_TWEETS).getCounter();
		FSProperty.writeInt(fs, nTweetsFile, nonSpamTweets);
		sLogger.info("num of non-spam tweets: " + nonSpamTweets);
		return nonSpamTweets;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 7) {
			printUsage();
			return;
		}
		String input = args[0];
		String output = args[1];
		int nReducers = Integer.parseInt(args[2]);
		String spamUserListFile = args[3];
		long startID = Long.parseLong(args[4]);
		long endID = Long.parseLong(args[5]);
		String nTweetsFile = args[6];
		Boolean spam = Boolean.parseBoolean(args[7]);
		removeTweetsOfSpamUsers(input, output, nReducers, spamUserListFile, startID, endID, nTweetsFile, spam);
	}
}
