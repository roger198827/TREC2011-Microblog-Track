package sa.edu.kaust.twitter.index.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListWritable;

public class TweetPostingsList implements Writable {

	ArrayListWritable<TweetPosting> list = null;
	int df = 0;
	int cf = 0;
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < list.size(); i++) {
			if (i != 0)
				sb.append(", ");
			sb.append(list.get(i).toString());
		}
		sb.append("]");

		return sb.toString();
	}
	
	public TweetPostingsList(){
		list = new ArrayListWritable<TweetPosting>();
	}
	
	public void add(long tweetID, short tf){
		list.add(new TweetPosting(tweetID, tf));
		df++;
		cf+=tf;
	}
	
	public void add(TweetPosting tp){
		list.add(tp);
		df++;
		cf+=tp.getTF();
	}

	public void clear(){
		list.clear();
		df = 0;
		cf = 0;
	}
	public void sort(){
		Collections.sort(list);
	}

	public PostingsReader getPostingsReader(){
		return new TweetPostingsReader(this); 
	}

	public int getDf(){
		return list.size();
	}

	public long getCf(){
		return cf;
	}

	public void readFields(DataInput in) throws IOException {
		list.readFields(in);
		df = list.size();
		cf = 0;
		for(TweetPosting tp : list) cf += tp.getTF(); 
	}

	public void write(DataOutput out) throws IOException {
		list.write(out);
	}
	
	public static class TweetPostingsReader implements PostingsReader{

		ArrayListWritable<TweetPosting> list;
		Iterator<TweetPosting> it;
		
		public TweetPostingsReader(TweetPostingsList l){
			list = l.list;
			it = l.list.iterator(); 
		}
		
		@Override
		public int getNumberOfPostings() {
			return list.size();
		}

		@Override
		public boolean hasMorePostings() {
			return it.hasNext();
		}

		@Override
		public boolean nextPosting(TweetPosting posting) {
			if(!it.hasNext()) return false;
			TweetPosting tp = it.next();
			posting.setTweetID(tp.getTweetID());
			posting.setTF(tp.getTF());
			return true;
		}

		@Override
		public void reset() {
			it = list.iterator();
			
		}
		
	}
}
