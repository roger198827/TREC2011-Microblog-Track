package sa.edu.kaust.twitter.preprocess;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import sa.edu.kaust.twitter.tokenize.TweetToken;
import sa.edu.kaust.twitter.tokenize.TweetTokenizer;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;
import sa.edu.kaust.utils.Util;

import com.twitter.corpus.data.HtmlStatus;
import com.twitter.corpus.data.Status;

//import edu.umd.cloud9.io.array.ValueListWritable;
import edu.umd.cloud9.io.pair.PairOfLongString;

public class CreateReplyCSV{

	private static final Logger sLogger = Logger.getLogger(CreateReplyCSV.class);

	private static class MyMapper extends Mapper<LongWritable, TweetWritable, LongWritable, Text> {

		private Text outValue = new Text();
		private LongWritable outKey = new LongWritable();
		public static long startID;
		public static long endID;

		public void setup(Mapper<LongWritable, TweetWritable, LongWritable, Text>.Context context) {
			startID= Long.parseLong(context.getConfiguration().get("startID"));
			endID= Long.parseLong(context.getConfiguration().get("endID"));
		}

		Set<String> replyToWhom;
		public void map(LongWritable key, TweetWritable value, Context context) throws IOException, InterruptedException {

			Tweet tweet = (Tweet)value;
			//System.out.println(tweet.getMessage());
			if (tweet.getID()>=startID && tweet.getID()<=endID)
			{
				if (Util.isreply(tweet.getMessage()))
				{				
					replyToWhom= TweetTokenizer.getReceivers(TweetTokenizer.getTokenStream(tweet.getMessage()));
					for(String r : replyToWhom){
						outKey.set(tweet.getID());
						outValue.set(r);
						context.write(outKey, outValue);
					}								
				}				

			}
		}
	}

	private static int printUsage() {
		System.out.println("usage: [input-dir] [output-dir]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	//@SuppressWarnings("unused")
	public static void createReplyCSV(String input, String output, String startID, String endID) throws Exception {

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
		//sLogger.info("num of output files: " + reduceTasks);

		int mapTasks = 100;

		Configuration conf = new Configuration();
		conf.set("startID", startID);
		conf.set("endID", endID);
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "Create CSV for Reply");
		job.setJarByClass(CreateReplyCSV.class);

		/*if (fs.exists(outputPath)) {
				sLogger.info("Output path already exist: skipping!");
				return;
			}*/

		//job.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//conf.set("mapred.child.java.opts", "-Xmx2048m");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);
		//job.setReducerClass(MyReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(output), true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		fs.copyToLocalFile(new Path(output+"/part-r-00000"), new Path("reply/part-r-00000"));
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0	+ " seconds");

	}

	public static void main(String[] args) throws Exception
	{
		createReplyCSV(args[0], args[1], args[2], args[3]); 
	}
}


