package sa.edu.kaust.twitter.preprocess.user;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfStringInt;


public class UserIndex {
	
	HashMap<String,ArrayListWritable<PairOfStringInt>>hm=new HashMap<String,ArrayListWritable<PairOfStringInt>>();
	@SuppressWarnings("unchecked")
	public UserIndex(Path indexPath,FileSystem fs) throws IOException {
		Configuration conf = new Configuration();				
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, indexPath, conf);
		Text key = new Text();
		ArrayListWritable<PairOfStringInt> value=new ArrayListWritable<PairOfStringInt>();
		int n=0;

		try {
			key = (Text) reader.getKeyClass().newInstance();
			value = (ArrayListWritable<PairOfStringInt>) reader.getValueClass().newInstance();

			while (reader.next(key, value)) {
				//System.out.println("Record " + n);
				//System.out.println("Key: " + key + "\nValue: " + value);
				//System.out.println("----------------------------------------");				
				hm.put(new String(key.toString()),new ArrayListWritable<PairOfStringInt>(value));
				n++;
				
			}
			reader.close();
			//System.out.println(n + " records read.\n");
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
		 Path path=new Path("E:\\CS240OS\\hadoop-0.20.2\\TermToUsername\\part-r-00000");
		 UserIndex test=new UserIndex(path,fs);		  
	  }
	


}
