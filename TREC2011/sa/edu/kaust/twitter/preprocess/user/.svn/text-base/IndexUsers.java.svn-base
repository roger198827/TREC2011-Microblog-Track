package sa.edu.kaust.twitter.preprocess.user;
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

import sa.edu.kaust.twitter.preprocess.hashtag.GetHashtagRepresentation;
import sa.edu.kaust.twitter.tokenize.*;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;
import sa.edu.kaust.twitter.data.TweetWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class IndexUsers extends Configured implements Tool {  
	private static Text mapperOutputKey=new Text();
	private static PairOfStringInt mapperOutputValue=new PairOfStringInt();
    public static class Map  
        extends Mapper<Text, ArrayListWritable<PairOfStringInt>, Text, PairOfStringInt> {

      public void map(Text key, ArrayListWritable<PairOfStringInt> value, Context context)  
          throws IOException, InterruptedException {    	   		  
	    	  for(int i=0;i<value.size();i++)
	    	  {		
	    		  mapperOutputKey.set(value.get(i).getLeftElement());
	    		  mapperOutputValue.set(key.toString(),value.get(i).getRightElement());
				  context.write(mapperOutputKey,mapperOutputValue); 
	    	  } 
      }  
    }  
  
    public static class Reduce  
    extends Reducer< Text, PairOfStringInt, Text, ArrayListWritable<PairOfStringInt>> {
    	private static  Text reducerOutputKey=new Text();  
    	private static  ArrayListWritable<PairOfIntString> temp= new ArrayListWritable<PairOfIntString>();
    	private static  ArrayListWritable<PairOfStringInt> reducerOutputValue=new ArrayListWritable<PairOfStringInt>();
    	//int frequencyThreshold;  
		//int topN;
		@Override
		/*protected void setup(Context context) throws IOException,
				InterruptedException {
			frequencyThreshold = context.getConfiguration().getInt("frequencyThreshold", 3);
			topN = context.getConfiguration().getInt("topN", 5);
		}*/
        public void reduce(Text key, Iterable<PairOfStringInt> values,  
          Context context) throws IOException, InterruptedException {  
    	  reducerOutputKey=key;
    	  HashMap<String,Integer>hm=new HashMap<String,Integer>(); 	    	  
    	  for(PairOfStringInt val:values)
    	  {    		 
    		  if(hm.containsKey(val.getLeftElement()))
    			  hm.put(val.getLeftElement(), hm.get(val.getLeftElement())+val.getRightElement());
    		  else
    			  hm.put(val.getLeftElement(), val.getRightElement());   			  
    	  }
    	  for(String hashKey:hm.keySet())
    	  {
    		  //if(hm.get(hashKey)>frequencyThreshold)
    		  temp.add(new PairOfIntString(hm.get(hashKey),hashKey));
    	  }
    	  Collections.sort(temp);
    	  for(int i=temp.size()-1;i>=0;i--)
    	  {
    		  //if(i==temp.size()-topN-1) break;    		  
    		  reducerOutputValue.add(new PairOfStringInt(temp.get(i).getRightElement(),temp.get(i).getLeftElement()));
    	  }
    	  if(reducerOutputValue.size()!=0)
    	  context.write(reducerOutputKey, reducerOutputValue);  
    	  temp.clear();
      	  reducerOutputValue.clear();
       }        	
	
    }    
    private static int printUsage() {
		//System.out.println("usage: [input-dir] [output-dir] [frequencyThreshold] [topN]");
    	System.out.println("usage: [input-dir] [output-dir]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
    private static final Logger sLogger = Logger.getLogger(GetHashtagRepresentation.class);
    public int run(String [] args) throws Exception {
      if (args.length != 2) {
			printUsage();
			return -1;
		}
      Configuration conf = new Configuration();
      //conf.setInt("frequencyThreshold", Integer.parseInt(args[2]));
	  //conf.setInt("topN", Integer.parseInt(args[3]));
      Job job = new Job(conf);  
      job.setJarByClass(IndexUsers.class);  
      job.setJobName("TermToUsername");  
      
      Path outputPath = new Path(args[1]);
      FileSystem fs = FileSystem.get(conf);
      //fs.delete(outputPath, true);
      if (fs.exists(outputPath)) {
			sLogger.info ("Output already exists: skipping!");
			return 1;
		}
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(PairOfStringInt.class);      
      job.setOutputKeyClass(Text.class);
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
    	new IndexUsers().run(args);
    }
    
    public static void indexUsers(String input, String output/*, int frequencyThreshold, int topN*/) throws Exception
    {
    	String[] args=new String[2];
    	args[0]=input;
    	args[1]=output;
    	//args[2]=String.valueOf(frequencyThreshold);
    	//args[3]=String.valueOf(topN);
    	new IndexUsers().run(args);
    }
 }  