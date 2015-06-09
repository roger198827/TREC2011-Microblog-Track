package sa.edu.kaust.twitter.preprocess.url;
import java.io.IOException; 
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.conf.*;  
import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapreduce.*;  
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;  
import org.apache.hadoop.mapreduce.lib.output.*;  
import org.apache.hadoop.util.*;  

import sa.edu.kaust.twitter.data.TweetWritable;
import sa.edu.kaust.twitter.tokenize.TweetToken;
import sa.edu.kaust.twitter.tokenize.TweetTokenizer;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;
import sa.edu.kaust.utils.UrlExtractionAndNormalization;


public class CreateURLTIDCSV extends Configured implements Tool {  

	public static class Map  
	extends Mapper<LongWritable,TweetWritable, LongWritable, IntWritable> {  
		LongWritable mapperOutputKey=new LongWritable();
		IntWritable mapperOutputValue=new IntWritable();
		public static long startID;
		public static long endID;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			startID= Long.parseLong(context.getConfiguration().get("startID"));
			endID= Long.parseLong(context.getConfiguration().get("endID"));
			super.setup(context);
		}

		public void map(LongWritable key, TweetWritable value, Context context)  
		throws IOException, InterruptedException { 

			if (value.getID()<startID || value.getID()>endID) return;
			ArrayList<TweetToken> tokens = TweetTokenizer.getTokenStream(value.getMessage());
			for(TweetToken token: tokens)
			{    		
				if(token.type == TweetTokenType.URL){
					mapperOutputKey.set(value.getID());
					mapperOutputValue.set(token.text.hashCode());
					context.write(mapperOutputKey, mapperOutputValue);
				}
			} 
		}  
	}  

	private static int printUsage() {
		System.out.println("usage: [input-dir] [output-dir]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	public int run(String [] args) throws Exception {  
		if (args.length != 4) {
			printUsage();
			return -1;
		}
		Configuration conf = new Configuration();
		conf.set("startID", args[2]);
		conf.set("endID", args[3]);

		Job job = new Job(conf);  
		job.setJarByClass(CreateURLTIDCSV.class);  
		job.setJobName("CreateURLTIDCSV");  

		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);       
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);  
		job.setOutputFormatClass(TextOutputFormat.class);  

		FileInputFormat.setInputPaths(job, new Path(args[0]));  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));    

		boolean success = job.waitForCompletion(true);  
		fs.copyToLocalFile(new Path(args[1]+"/part-r-00000"), new Path("urltid/part-r-00000"));
		return success ? 0 : 1;  
	}  

	public static void RunCreateURLTIDCSV(String input, String output, String startID, String endID) throws Exception
	{
		String[] args=new String[4];
		args[0]=input;
		args[1]=output;
		args[2]=startID;
		args[3]=endID;
		int ret = ToolRunner.run(new CreateURLTIDCSV(), args);   
	}

	public static void main(String[] args) throws Exception
	{
		int ret = ToolRunner.run(new CreateURLTIDCSV(), args);   
	}
}  