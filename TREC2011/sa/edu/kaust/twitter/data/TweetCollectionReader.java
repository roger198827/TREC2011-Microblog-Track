package sa.edu.kaust.twitter.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.twitter.corpus.data.HtmlStatusBlockReader;
import com.twitter.corpus.data.Status;
import com.twitter.corpus.data.StatusStream;

/**
 * Abstraction for a corpus of statuses. A corpus is assumed to consist of a number of blocks, each
 * represented by SequenceFile, under a single directory. This object allows the caller to read
 * through all blocks, in sorted lexicographic order of the files.
 */
public class TweetCollectionReader implements TweetStream {
	private final FileStatus[] files;
	private final FileSystem fs;

	private int nextFile = 0;
	private TweetBlockReader currentBlock = null;

	public TweetCollectionReader(Path directory/*, FileSystem fs*/) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Preconditions.checkNotNull(directory);
		this.fs = Preconditions.checkNotNull(fs);
		if (!fs.getFileStatus(directory).isDir()) {
			throw new IOException("Expecting " + directory + " to be a directory!");
		}

		files = fs.listStatus(directory);

		if (files.length == 0) {
			throw new IOException(directory + " does not contain any files!");
		}
	}

	/**
	 * Returns the next status, or <code>null</code> if no more statuses.
	 */
	public Tweet next() throws IOException {
		if (currentBlock == null) {
			currentBlock = new TweetBlockReader(files[nextFile].getPath(), fs);
			nextFile++;
		}

		Tweet status = null;
		while (true) {
			status = currentBlock.next();
			if (status != null) {
				return status;
			}

			if (nextFile >= files.length) {
				// We're out of files to read. Must be the end of the corpus.
				return null;
			}

			currentBlock.close();
			// Move to next file.
			currentBlock = new TweetBlockReader(files[nextFile].getPath(), fs);
			nextFile++;
		}
	}

	public void close() throws IOException {
		currentBlock.close();
	}
}
