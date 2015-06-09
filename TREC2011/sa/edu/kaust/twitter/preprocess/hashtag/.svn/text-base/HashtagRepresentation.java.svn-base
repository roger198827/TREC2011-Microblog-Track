package sa.edu.kaust.twitter.preprocess.hashtag;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;




import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfStringInt;


public class HashtagRepresentation {
	
	HashMap<String,ArrayListWritable<PairOfStringInt>>hm=new HashMap<String,ArrayListWritable<PairOfStringInt>>();
	@SuppressWarnings("unchecked")
	public HashtagRepresentation(Path indexPath,FileSystem fs) throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, indexPath, fs.getConf());
		Text key = new Text();
		ArrayListWritable<PairOfStringInt> value=new ArrayListWritable<PairOfStringInt>();
		int n=0;

		try {
			key = (Text) reader.getKeyClass().newInstance();
			value = (ArrayListWritable<PairOfStringInt>) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
			/*	System.out.println("Record " + n);
				System.out.println("Key: " + key + "\nValue: " + value);
				System.out.println("----------------------------------------");*/				
				hm.put(new String(key.toString()),new ArrayListWritable<PairOfStringInt>(value));
				n++;
				
			}
			reader.close();
			System.out.println(n + " records read.\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
      
		
	}
	public ArrayListWritable<PairOfStringInt> getValue(String term) throws IOException
	{
		
		if (hm.get(term)!=null)
			return hm.get(term);
		else
			return null;
		
	}
	public static void main(String[] args) throws Exception { 
		 Configuration conf=new Configuration();
		 FileSystem fs=FileSystem.get(conf);	
		 Path path=new Path("E:\\CS240OS\\hadoop-0.20.2\\HashtagRepresentation\\part-r-00000");
		 HashtagRepresentation test=new HashtagRepresentation(path,fs);		  
	     //UploadHashtagRepresentation test=new UploadHashtagRepresentation(args[0]);
	    // System.out.println(test.getValue("teamfollowback"));     
	  }
	


}
