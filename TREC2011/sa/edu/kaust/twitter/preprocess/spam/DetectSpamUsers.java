package sa.edu.kaust.twitter.preprocess.spam;
import java.io.IOException; 
import java.util.*;    

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.conf.*;  
import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapreduce.*;  
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;  
import org.apache.hadoop.mapreduce.lib.output.*;  
import org.apache.hadoop.util.*;  
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.map.HMapII;

import sa.edu.kaust.twitter.data.TweetWritable;
import sa.edu.kaust.twitter.index.BuildTweetsForwardIndex;
import sa.edu.kaust.twitter.tokenize.TweetToken;
import sa.edu.kaust.twitter.tokenize.TweetTokenizer;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;


public class DetectSpamUsers{  

	public static class Map extends Mapper<LongWritable,TweetWritable, Text, IntWritable> {
		public static long startID;
		public static long endID;

		protected void setup(Context context) throws IOException,
		InterruptedException {
			startID = context.getConfiguration().getLong("startID", 0);
			endID = context.getConfiguration().getLong("endID", 0);
		}

		Text mapperOutputKey = new Text();
		IntWritable mapperOutputValue = new IntWritable();
		public void map(LongWritable key, TweetWritable value, Context context)  
		throws IOException, InterruptedException {

			if (value.getID()<startID || value.getID()>endID) return;

			ArrayList<TweetToken> tokens = TweetTokenizer.getTokenStream(value.getMessage());
			for(TweetToken token: tokens)
			{    		
				if(token.type.equals(TweetTokenType.URL)){
					mapperOutputKey.set(value.getUserName());
					mapperOutputValue.set(token.text.hashCode());
					context.write(mapperOutputKey, mapperOutputValue);
				}
			}    
		}  
	}  

	public static class Reduce  
	extends Reducer<Text, IntWritable, Text, IntWritable> {     	
		int spamUrlThreshold=3;  //spam URL threshold 
		int spamUserThreshold=3;
		int strictSpamUrlThreshold=5;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			spamUrlThreshold = context.getConfiguration().getInt("spamUrlThreshold", 3);
			spamUserThreshold = context.getConfiguration().getInt("spamUserThreshold", 3);
			strictSpamUrlThreshold=context.getConfiguration().getInt("strictSpamUrlThreshold", 5);
		}
		private static IntWritable reducerOutputValue= new IntWritable();
		//HashMap<Integer,Integer>hm = new HashMap<Integer,Integer>();
		HMapII map = new HMapII();
		public void reduce(Text key, Iterable<IntWritable> values,  
				Context context) throws IOException, InterruptedException {  
			
			map.clear();
			int count=0; 
			for(IntWritable val:values)
			{
				if(map.containsKey(val.get()))
					map.put(val.get(), map.get(val.get())+1);
				else
					map.put(val.get(), 1); 
			}

			for(int hashKey:map.keySet())
			{
				int times=map.get(hashKey);
				if(times>=strictSpamUrlThreshold)
				{
					reducerOutputValue.set(times);
					context.write(key, reducerOutputValue);
				}
				else if(map.get(hashKey)>=spamUrlThreshold)
					count++;
			}
			if(count>=spamUserThreshold)      
			{
				reducerOutputValue.set(count);
				context.write(key, reducerOutputValue);

			}
		}
	}    

	private static final Logger sLogger = Logger.getLogger(DetectSpamUsers.class);
	public static void detectSpamUsers(String input, String output, 
			long startID, long endID, int spamUrlThreshold, int spamUserThreshold,int strictSpamUrlThreshold) throws Exception {
		
		Configuration conf = new Configuration();
		conf.setLong("startID", startID);
		conf.setLong("endID", endID);
		conf.setInt("spamUrlThreshold", spamUrlThreshold);
		conf.setInt("spamUserThreshold", spamUserThreshold);
		conf.setInt("strictSpamUrlThreshold", strictSpamUrlThreshold);
		

		Job job = new Job(conf);  
		job.setJarByClass(DetectSpamUsers.class);  
		job.setJobName("DetectSpamUsers");  

		Path outputPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		//fs.delete(outputPath, true);
		if (fs.exists(outputPath)) {
			sLogger.info ("Output already exists: skipping!");
			return;
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);      
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);      

		job.setMapperClass(Map.class);  
		//   job.setCombinerClass(Reduce.class);  
		job.setReducerClass(Reduce.class);  
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);  
		job.setOutputFormatClass(TextOutputFormat.class);  

		FileInputFormat.setInputPaths(job, new Path(input));  
		FileOutputFormat.setOutputPath(job, outputPath);    

		job.waitForCompletion(true);  
	}  

	private static int printUsage() {
		System.out.println("usage: [input-dir] [output-dir] [startID] [endID] [spamUrlThreshold] [spamUserThreshold] [strictSpamUrlThreshold]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 7) {
			printUsage();
			return;
		}
		String input = args[0];
		String output = args[1];
		long startID = Long.parseLong(args[2]);
		long endID = Long.parseLong(args[3]);
		int spamUrlThreshold= Integer.parseInt(args[4]);
		int spamUserThreshold= Integer.parseInt(args[5]);
		int strictSpamUrlThreshold= Integer.parseInt(args[6]);
		detectSpamUsers(input, output, startID, endID, spamUrlThreshold, spamUserThreshold,strictSpamUrlThreshold);
	}
}  