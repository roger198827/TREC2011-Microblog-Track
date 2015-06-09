package sa.edu.kaust.twitter.preprocess;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sa.edu.kaust.twitter.data.Tweet;
import sa.edu.kaust.twitter.data.TweetWritable;
import sa.edu.kaust.twitter.preprocess.DetectRetweets.Tweets;
import sa.edu.kaust.twitter.tokenize.TweetToken;
import sa.edu.kaust.twitter.tokenize.TweetTokenizer;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;
import sa.edu.kaust.utils.Util;

import com.twitter.corpus.data.HtmlStatus;
import com.twitter.corpus.data.Status;

//import edu.umd.cloud9.io.array.ValueListWritable;
import edu.umd.cloud9.io.pair.PairOfLongString;

public class CreateTweetCSV{

		private static final Logger sLogger = Logger.getLogger(CreateTweetCSV.class);
		
		
		private static class MyMapper extends Mapper<LongWritable, TweetWritable, LongWritable, Text> {
			
			private Text outValue = new Text();
			private LongWritable outKey = new LongWritable();
			//public int count =0;
			public static long startID;
			public static long endID;
			 
			public void setup(Mapper<LongWritable, TweetWritable, LongWritable, Text>.Context context, JobConf job) {
				startID= Long.parseLong(job.get("startID").toString());
				endID= Long.parseLong(job.get("endID").toString());
			}
			
			public void map(LongWritable key, TweetWritable value, Context context) throws IOException, InterruptedException {
				Tweet tweet = (Tweet)value;
				
				if (tweet.getID()>=startID && tweet.getID()<=endID)
				{				
					//System.out.println(tweet.getID()+" in between "+startID+" and "+endID);
					outKey.set(tweet.getID());
					outValue.set(tweet.getUserName());
					context.write(outKey, outValue);					
				}//else if(count<10)
					//System.out.println(tweet.getID()+" isn't between "+startID+" and "+endID);
				//count++;
		}
	 }
		
		private static class MyReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
			private Text outValue = new Text();
			private LongWritable outKey = new LongWritable();

			public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				Iterator<Text> it = values.iterator();
				outKey.set(key.get());
				while(it.hasNext()){
					outValue.set(it.next());
				}
				context.write(outKey, outValue);
			}
		}
			private static int printUsage() {
			System.out.println("usage: [input-dir] [output-dir] [numOfReducer]");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		//@SuppressWarnings("unused")
		public static void createTweetCSV(String input, String output, int reduceTasks, String startID, String endID) throws Exception {

			/*if (args.length != 2) {
				System.out.println(args.length);
				printUsage();
				return;
			}
			
			String input = args[0];
			String output = args[1];*/
			//int reduceTasks = Integer.parseInt(args[2]);
			System.out.println(input+" "+output);
			//Path inputPath = new Path("/shared/tweets2011");
			//Path outputPath = new Path("/user/telsayed/tweets2011");
			
			Path inputPath = new Path(input);
			Path outputPath = new Path(output);
			
			sLogger.info("input dir: " + inputPath);
			sLogger.info("output dir: " + outputPath);
			sLogger.info("num of output files: " + reduceTasks);

			int mapTasks = 100;

			Configuration conf = new Configuration();
			conf.set("startID", startID);
			conf.set("endID", endID);
			FileSystem fs = FileSystem.get(conf);
			Job job = new Job(conf, "Create CSV for Tweet");
			job.setJarByClass(CreateTweetCSV.class);
			
			/*if (fs.exists(outputPath)) {
				sLogger.info("Output path already exist: skipping!");
				return;
			}*/

			job.setNumReduceTasks(reduceTasks);
			
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			//conf.set("mapred.child.java.opts", "-Xmx2048m");

			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			//job.setMapOutputKeyClass(LongWritable.class);
			//job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			
			// delete the output directory if it exists already
			FileSystem.get(conf).delete(new Path(output), true);
			
			long startTime = System.currentTimeMillis();
			job.waitForCompletion(true);
			for(int y=0;y<reduceTasks;y++)
			{
				fs.copyToLocalFile(new Path(output+"/part-r-0000"+y), new Path("tweet/part-r-0000"+y));
			}
			sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0	+ " seconds");

		}
		
		public static void main(String[] args) throws NumberFormatException, Exception
		{
			createTweetCSV(args[0], args[1], Integer.parseInt(args[2]), args[3], args[4]);
		}
	}


