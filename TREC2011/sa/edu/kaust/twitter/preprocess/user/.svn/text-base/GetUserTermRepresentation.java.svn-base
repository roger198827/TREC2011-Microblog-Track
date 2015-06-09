package sa.edu.kaust.twitter.preprocess.user;
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
import edu.umd.cloud9.io.FSProperty;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.io.pair.PairOfStringInt;

public class GetUserTermRepresentation extends Configured {

	protected static enum Statistics {
		NUM_OF_USERS;
	}

	public static Text mapperOutputKey=new Text();
	public static Text mapperOutputValue=new Text();
	public static class Map  
	extends Mapper<LongWritable,TweetWritable, Text, Text> {
		public static long startID;
		public static long endID;
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
		throws IOException, InterruptedException {
			startID= Long.parseLong(context.getConfiguration().get("startID"));
			endID= Long.parseLong(context.getConfiguration().get("endID"));
			super.setup(context);
		}

		public void map(LongWritable key, TweetWritable value, Context context)  
		throws IOException, InterruptedException { 

			if (value.getID()>=startID && value.getID()<=endID)
			{
				String tweet=value.getMessage();       	       		  
				ArrayList<TweetToken> tokens= TweetTokenizer.getTokenStream(tweet);
				for(int i=0;i<tokens.size();i++)
				{
					TweetTokenType type=tokens.get(i).type;
					if(type.equals(TweetTokenType.OTHER))
					{
						mapperOutputKey.set(value.getUserName());
						mapperOutputValue.set(tokens.get(i).text);
						context.write(mapperOutputKey, mapperOutputValue); 
					}
				}
			}    	
		}  
	}  

	public static class Reduce  
	extends Reducer< Text, Text, Text, ArrayListWritable<PairOfStringInt>> {
		private static  Text reducerOutputKey=new Text();  
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
		public void reduce(Text key, Iterable<Text> values,  
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
			if(reducerOutputValue.size()!=0){
				context.write(reducerOutputKey, reducerOutputValue);
				context.getCounter(Statistics.NUM_OF_USERS).increment(1);
			}
			temp.clear();
			reducerOutputValue.clear();
		}        
	}    
	private static int printUsage() {
		System.out.println("usage: [input-dir] [output-dir] [startID] [endID] [frequencyThreshold] [topN] [numOfUserFile]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	private static final Logger sLogger = Logger.getLogger(GetUserTermRepresentation.class);
	public int run(String [] args) throws Exception {
		if (args.length != 7) {
			printUsage();
			return -1;
		}
		Configuration conf = new Configuration();
		conf.set("startID", args[2]);
		conf.set("endID", args[3]);
		conf.setInt("frequencyThreshold", Integer.parseInt(args[4]));
		conf.setInt("topN", Integer.parseInt(args[5]));
		// Job job = new Job(getConf());  
		Job job = new Job(conf);  
		job.setJarByClass(GetUserTermRepresentation.class);  
		job.setJobName("UsernameToTerm");  

		Path outputPath = new Path(args[1]);
		String numOfUserFile = args[6];
		//FileSystem fs = FileSystem.get(getConf());
		FileSystem fs = FileSystem.get(conf);
		//fs.delete(outputPath, true);
		if (fs.exists(outputPath)) {
			sLogger.info ("Output already exists: skipping!");
			return 1;
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);      
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayListWritable.class);


		job.setMapperClass(Map.class);  
		//   job.setCombinerClass(Reduce.class);  
		job.setReducerClass(Reduce.class);  
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);  
		job.setOutputFormatClass(SequenceFileOutputFormat.class);  

		FileInputFormat.setInputPaths(job, new Path(args[0]));  
		FileOutputFormat.setOutputPath(job, outputPath);    

		boolean success = job.waitForCompletion(true);  
		//return success ? 0 : 1;  

		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		// Write out number of postings. NOTE: this value is not the same as
		// number of postings, because postings for non-English terms are
		// discarded, or as result of df cut.
		int nUsers =(int) counters.findCounter(Statistics.NUM_OF_USERS).getValue();

		FSProperty.writeInt(fs, numOfUserFile, nUsers);

		return nUsers;
	}  

	public static void main(String[] args) throws Exception {  
		int ret = new GetUserTermRepresentation().run(args);  
		//System.exit(ret);  
	} 

	public static int getUserTermRepresentation(String input, String output,String startID, String endID,int frequencyThreshold, int topN, String numOfUserFile) throws Exception
	{
		String[] args=new String[7];
		args[0]=input;
		args[1]=output;    	
		args[2]=startID;
		args[3]=endID;
		args[4]=String.valueOf(frequencyThreshold);
		args[5]=String.valueOf(topN);
		args[6]=numOfUserFile; 
		return new GetUserTermRepresentation().run(args);  
	}
}  