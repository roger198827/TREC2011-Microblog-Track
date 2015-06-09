package sa.edu.kaust.twitter.data;

import java.io.IOException;


/**
 * Abstraction for a stream of statuses. Ordering of the statuses is left to the implementation.
 */
public interface TweetStream {
  public Tweet next() throws IOException;
  public void close() throws IOException;
}
