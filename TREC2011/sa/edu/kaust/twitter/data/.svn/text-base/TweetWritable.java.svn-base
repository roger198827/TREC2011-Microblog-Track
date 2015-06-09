package sa.edu.kaust.twitter.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class TweetWritable extends Tweet implements Writable{

	public TweetWritable(long iD, String userName, int code, String timeStamp,
			String message) {
		super(iD, userName, code, timeStamp, message);
	}

	public TweetWritable(){

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readLong();
		code = in.readInt();
		userName = in.readUTF();
		timeStamp = in.readUTF();
		message = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeInt(code);
		out.writeUTF(userName);
		//if(code != 404 && code != 403){
		//if(timeStamp!=null)	
			out.writeUTF(timeStamp);
		//else out.writeUTF("null");
		//if(message!=null) 
			out.writeUTF(message);	
		//else out.writeUTF("null");
	}
	
	public boolean isNull(){
		if(timeStamp==null || message==null) return true;
		else return false; 
	}
}
