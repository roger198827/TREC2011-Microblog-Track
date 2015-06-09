package sa.edu.kaust.twitter.preprocess.url;


import java.io.IOException;

import java.util.*;    

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.conf.*;  
import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapreduce.*;  
import org.apache.hadoop.mapreduce.lib.input.*;  
import org.apache.hadoop.mapreduce.lib.output.*;  
import org.apache.hadoop.util.*;  
import org.apache.log4j.Logger;

import sa.edu.kaust.twitter.tokenize.*;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;
import sa.edu.kaust.twitter.data.TweetWritable;
import sa.edu.kaust.twitter.index.BuildTweetsForwardIndex;
import sa.edu.kaust.utils.UrlExtractionAndNormalization;
import edu.umd.cloud9.io.FSProperty;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import java.util.Collections;


public class GetUrlRepresentation extends Configured implements Tool {  

	public static class Map  
	extends Mapper<LongWritable,TweetWritable, IntWritable, Text> {
		@Override
		protected void setup(Context context)
		throws IOException, InterruptedException {
			startID= Long.parseLong(context.getConfiguration().get("startID"));
			endID= Long.parseLong(context.getConfiguration().get("endID"));
		}
		public static long startID;
		public static long endID;

		public static IntWritable mapperOutputKey=new IntWritable();
		public static Text mapperOutputValue=new Text();   

		public void map(LongWritable key, TweetWritable value, Context context)  
		throws IOException, InterruptedException { 

			if (value.getID()<startID || value.getID()>endID) return;
			String tweet=value.getMessage();    	 
			ArrayList<TweetToken> tokens= TweetTokenizer.getTokenStream(tweet);
			ArrayList<String>url=new ArrayList<String>(); 
			// Add all urls to an Arraylist
			if(UrlExtractionAndNormalization.extractURL(tweet) != null){
				for(int i=0;i<tokens.size();i++)
				{    			   
					if(tokens.get(i).type.equals(TweetTokenType.URL))
						url.add(tokens.get(i).text);	    	  
				}    
			}

			for(int j=0;j<url.size();j++)
			{ 
				for(int k=0;k<tokens.size();k++)
				{
					TweetTokenType type=tokens.get(k).type;
					if(type.equals(TweetTokenType.OTHER)){    				  
						mapperOutputKey.set(url.get(j).hashCode());
						mapperOutputValue.set(tokens.get(k).text);
						context.write(mapperOutputKey, mapperOutputValue); 
					}

				}
			} 
		}  
	}  

	public static class Reduce  
	extends Reducer< IntWritable, Text, IntWritable, ArrayListWritable<PairOfStringInt>> {
		private static  IntWritable reducerOutputKey=new IntWritable();  
		private static  ArrayListWritable<PairOfIntString> temp= new ArrayListWritable<PairOfIntString>();
		private static  ArrayListWritable<PairOfStringInt> reducerOutputValue=new ArrayListWritable<PairOfStringInt>();
		int frequencyThreshold;  
		int topN;
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			frequencyThreshold = context.getConfiguration().getInt("frequencyThreshold", 3);
			topN = context.getConfiguration().getInt("topN", 5);
		}
		public void reduce(IntWritable key, Iterable<Text> values,  
				Context context) throws IOException, InterruptedException {  
			reducerOutputKey=key;
			HashMap<String,Integer>hm=new HashMap<String,Integer>(); 	    	  
			for(Text val:values)
			{    		 
				if(hm.containsKey(val.toString()))
					hm.put(val.toString(), hm.get(val.toString())+1);
				else
					hm.put(val.toString(), 1);   			  
			}
			for(String hashKey:hm.keySet())
			{
				if(hm.get(hashKey)>=frequencyThreshold)
					temp.add(new PairOfIntString(hm.get(hashKey),hashKey));
			}
			Collections.sort(temp);
			for(int i=temp.size()-1;i>=0;i--)
			{
				if(i==temp.size()-topN-1)
					break;    		  
				reducerOutputValue.add(new PairOfStringInt(temp.get(i).getRightElement(),temp.get(i).getLeftElement()));
			}
			if(reducerOutputValue.size()!=0)
				context.write(reducerOutputKey, reducerOutputValue);  
			temp.clear();
			reducerOutputValue.clear();
		}        

	}    
	private static int printUsage() {
		System.out.println("usage: [input-dir] [output-dir][startID] [endID] [frequencyThreshold] [topN]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	private static final Logger sLogger = Logger.getLogger(GetUrlRepresentation.class);
	public int run(String [] args) throws Exception {
		if (args.length != 6) {
			printUsage();
			return -1;
		}

		Configuration conf = new Configuration();
		conf.set("startID", args[2]);
		conf.set("endID", args[3]);
		conf.setInt("frequencyThreshold", Integer.parseInt(args[4]));
		conf.setInt("topN", Integer.parseInt(args[5]));

		//Job job = new Job(getConf());  
		Job job = new Job(conf);
		job.setJarByClass(GetUrlRepresentation.class);  
		job.setJobName("UrlRepresentation");  

		Path outputPath = new Path(args[1]);
		//FileSystem fs = FileSystem.get(getConf());
		FileSystem fs = FileSystem.get(conf);
		//fs.delete(outputPath, true);
		if (fs.exists(outputPath)) {
			sLogger.info ("Output already exists: skipping!");
			return 1;
		}

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);      
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ArrayListWritable.class);

		job.setMapperClass(Map.class);  
		//   job.setCombinerClass(Reduce.class);  
		job.setReducerClass(Reduce.class);  
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);  
		job.setOutputFormatClass(SequenceFileOutputFormat.class);  

		FileInputFormat.setInputPaths(job, new Path(args[0]));  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));    

		boolean success = job.waitForCompletion(true);  
		return success ? 0 : 1;  
	}  

	public static void main(String[] args) throws Exception {  
		int ret = ToolRunner.run(new GetUrlRepresentation(), args);  
		//System.exit(ret);  
	}  

	public static void getURLRepresentation(String input, String output, String startID, String endID,int frequencyThreshold, int topN) throws Exception
	{
		String[] args=new String[6];
		args[0]=input;
		args[1]=output;    	
		args[2]=startID;
		args[3]=endID;
		args[4]=String.valueOf(frequencyThreshold);
		args[5]=String.valueOf(topN);
		int ret = ToolRunner.run(new GetUrlRepresentation(), args);  
	}
}  
