/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sa.edu.kaust.twitter.data.TweetWritable;
import sa.edu.kaust.twitter.index.data.TweetPostingsList;

import edu.umd.cloud9.io.array.ArrayListWritable;

@SuppressWarnings("deprecation")
public class BuildTweetsForwardIndex extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(BuildTweetsForwardIndex.class);

	protected static enum Dictionary {
		Size
	};

	private static class MyMapRunner implements
			MapRunnable<LongWritable, TweetWritable, LongWritable, Text> {

		private String mInputFile;
		private Text outputValue = new Text();

		public void configure(JobConf job) {
			mInputFile = job.get("map.input.file"); // get the filename of the split
		}

		public void run(RecordReader<LongWritable, TweetWritable> input,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			LongWritable key = input.createKey();
			TweetWritable value = input.createValue();
			int fileNo = Integer.parseInt(mInputFile.substring(mInputFile.lastIndexOf("-") + 1)); // get file no
			//if(fileNo != 6 && fileNo!=20) return;
			long pos = input.getPos(); // get curent position
			while (input.next(key, value)) {
				outputValue.set(fileNo + "\t" + pos);
				output.collect(key, outputValue);
				reporter.incrCounter(Dictionary.Size, 1);

				pos = input.getPos();
			}
			sLogger.info("last termid: " + key + "(" + fileNo + ", " + pos + ")");
		}
	}

	public static final long BigNumber = 1000000000;

	private static class MyReducer extends MapReduceBase implements
			Reducer<LongWritable, Text, Text, Text> {

		FSDataOutputStream mOut;

		//int mCount;
		int mCurDoc = 0;

		public void configure(JobConf job) {
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
			} catch (Exception e) {
				throw new RuntimeException("Error opening the FileSystem!");
			}

			//String indexPath = job.get("Ivory.IndexPath");

			String forwardIndexPath = job.get("ForwardIndexPath");
			//mCount = job.getInt("Count", -1);

			try {
				mOut = fs.create (new Path (forwardIndexPath), true);
				//mOut.writeInt(mCount);
			} catch (Exception e) {
				throw new RuntimeException("Error in creating files!");
			}

		}

		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] s = values.next().toString().split("\\s+");

			//sLogger.info (key + ": " + s[0] + " " + s[1]);
			if (values.hasNext())
				throw new RuntimeException("There shouldn't be more than one value, key=" + key);

			int fileNo = Integer.parseInt(s[0]);
			long filePos = Long.parseLong(s[1]);
			long pos = BigNumber * fileNo + filePos;

			mCurDoc++;
			mOut.writeLong(key.get());
			mOut.writeLong(pos);
		}

		public void close() throws IOException {
			mOut.close();

			/*if (mCurDoc != mCount) {
				throw new IOException("Expected " + mCount
						+ " docs, actually got " + mCurDoc + " terms!");
			}*/
		}
	}

	private static int printUsage() {
		System.out.println("usage: [input] [output-dir]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			printUsage();
			return -1;
		}
		
		JobConf conf = new JobConf(BuildTweetsForwardIndex.class);
		FileSystem fs = FileSystem.get(conf);

		int mapTasks = 10;
		sLogger.info("Tool: TweetsForwardIndex");

		String postingsPath = args[0];
		String forwardIndexPath = args[1];

		if (!fs.exists(new Path(postingsPath))) {
			sLogger.info("Error: IntDocVectors don't exist!");
			return 0;
		}

		// delete the output directory if it exists already
		//FileSystem.get(conf).delete(new Path(forwardIndexPath), true);
		if (fs.exists (new Path (forwardIndexPath))) {
			sLogger.info ("PostingsForwardIndex already exists: skipping!");
			return 0;
		}

		conf.set("ForwardIndexPath", forwardIndexPath);
		
		conf.setJobName("BuildTweetsForwardIndex");

		Path inputPath = new Path(postingsPath);
		FileInputFormat.setInputPaths(conf, inputPath);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);

		conf.set("mapred.child.java.opts", "-Xmx2048m");

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputFormat(NullOutputFormat.class);

		conf.setMapRunnerClass(MyMapRunner.class);
		conf.setReducerClass(MyReducer.class);

		JobClient.runJob(conf);

		return 0;
	}
	
	public static void buildTweetsForwardIndex(String postingsPath, String forwardIndexPath) throws Exception{
		String[] args = new String[2];
		args[0]= postingsPath;
		args[1]= forwardIndexPath;
		new BuildTweetsForwardIndex().run(args);
	}
	
	public static void main(String[] args) throws Exception {
		new BuildTweetsForwardIndex().run(args);
	}
	
	
}
