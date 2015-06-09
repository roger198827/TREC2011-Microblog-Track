package sa.edu.kaust.twitter.tokenize;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import sa.edu.kaust.twitter.data.Tweet;
import sa.edu.kaust.twitter.data.TweetBlockReader;


public class TestRetweet {

	public static void main(String[] args) throws IOException {
		Text reTweetContent = new Text();
		
		/*String test = "RT @silmiapin: RT @TwitFAKTA: faktanya lebih tahan main twitter berjam-jam daripada duduk baca buku pelajaran. #TwitFAKTA";
		System.out.println(test);
		
		if(TweetTokenizer.detectRetweet(test, reTweetContent)){
			System.out.println(">>>>");
			System.out.println(reTweetContent.toString());
		}
		if(true) return;*/
		String filePath = "data/sequence/part-r-00004";
		//TweetTokenizer tweetTokenizer = new TweetTokenizer();
		TweetBlockReader tweetSeqFileReader = new TweetBlockReader(new Path(filePath));
		Tweet tweet = tweetSeqFileReader.next();
		int i = 0;
		while(tweet !=null){
			i++;
			System.out.println(i+"-"+tweet);
			ArrayList<TweetToken> tokens = TweetTokenizer.getTokenStream(tweet.getMessage());
			if(TweetTokenizer.detectRetweet(tweet, reTweetContent, tokens)){
				System.out.println(">>>>");
				System.out.println(reTweetContent.toString());
			}
			//System.out.println(TweetTokenizer.getStringStream(TweetTokenizer.getTokenStream(tweet.getMessage())));
			tweet = tweetSeqFileReader.next();
		}
		tweetSeqFileReader.close();
	}

}
