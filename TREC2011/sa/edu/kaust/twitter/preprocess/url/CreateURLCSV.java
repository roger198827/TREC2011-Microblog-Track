package sa.edu.kaust.twitter.preprocess.url;
import java.io.IOException; 
import java.util.*;    

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
import edu.umd.cloud9.io.pair.*;

public class CreateURLCSV extends Configured implements Tool {  

	public static long startID;
	public static long endID;

	public static class Map  
	extends Mapper<LongWritable,TweetWritable, IntWritable, Text> {   
		IntWritable mapperOutputKey=new IntWritable();
		Text mapperOutputValue=new Text();

		@Override    	 
		protected void setup(Context context) throws IOException,
		InterruptedException {
			startID= Long.parseLong(context.getConfiguration().get("startID"));
			endID= Long.parseLong(context.getConfiguration().get("endID"));
			super.setup(context);
		}

		public void map(LongWritable key, TweetWritable value, Context context)  
		throws IOException, InterruptedException { 

			if (value.getID()>=startID && value.getID()<=endID)
			{
				ArrayList<TweetToken> tokens = TweetTokenizer.getTokenStream(value.getMessage());
				for(TweetToken token: tokens)
				{    		
					if(token.type == TweetTokenType.URL){
						mapperOutputKey.set(token.text.hashCode());
						mapperOutputValue.set(token.text);
						context.write(mapperOutputKey, mapperOutputValue);
					}
				} 
			}
		}  
	}  

	public static class Reduce  
	extends Reducer<IntWritable, Text, IntWritable, Text> {    	

		public void reduce(IntWritable key, Iterable<Text> values,  
				Context context) throws IOException, InterruptedException {  

			context.write(key, values.iterator().next());     
		}        

	}    
	private static int printUsage() {
		System.out.println("usage of CreateURLCSV: [input-dir] [output-dir] [startID] [endID]");
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
		job.setJarByClass(CreateURLCSV.class);  
		job.setJobName("CreateURLCSV");  

		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);      
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);


		job.setMapperClass(Map.class);  
		//   job.setCombinerClass(Reduce.class);  
		job.setReducerClass(Reduce.class);  
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);  
		job.setOutputFormatClass(TextOutputFormat.class);  

		FileInputFormat.setInputPaths(job, new Path(args[0]));  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));          
		boolean success = job.waitForCompletion(true);  
		fs.copyToLocalFile(new Path(args[1]+"/part-r-00000"), new Path("url/part-r-00000"));
		return success ? 0 : 1;  
	}  

	public static void createURLCSV(String input,String output, String startID, String endID) throws Exception { 
		String[] args=new String[4];
		args[0]=input;
		args[1]=output;
		args[2]=startID;
		args[3]=endID;
		int ret = ToolRunner.run(new CreateURLCSV(), args);   
	}  

	public static void main(String[] args) throws Exception
	{
		int ret = ToolRunner.run(new CreateURLCSV(), args);   
	}
}  