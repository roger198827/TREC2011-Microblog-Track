package sa.edu.kaust.twitter.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import com.google.common.base.Preconditions;
import com.twitter.corpus.data.HtmlStatus;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;

import edu.umd.cloud9.io.pair.PairOfLongString;

/**
 * Abstraction for a stream of statuses, backed by an underlying SequenceFile.
 */
public class TweetBlockReader implements TweetStream {
	private final SequenceFile.Reader reader;

	private final LongWritable key = new LongWritable();
	private final TweetWritable value = new TweetWritable();

	public TweetBlockReader(Path path/*, FileSystem fs*/) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Preconditions.checkNotNull(path);
		Preconditions.checkNotNull(fs);
		reader = new SequenceFile.Reader(fs, path, fs.getConf());
	}
	
	public TweetBlockReader(Path path, FileSystem fs) throws IOException {
		Preconditions.checkNotNull(path);
		Preconditions.checkNotNull(fs);
		reader = new SequenceFile.Reader(fs, path, fs.getConf());
	}

	/**
	 * Returns the next status, or <code>null</code> if no more statuses.
	 */
	public Tweet next() throws IOException {
		if (!reader.next(key, value))
			return null;

		return (Tweet)value;
	}

	public void close() throws IOException {
		reader.close();
	}
}
