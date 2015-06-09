package sa.edu.kaust.twitter.tokenize;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import edu.umd.cloud9.io.pair.PairOfStringInt;

import sa.edu.kaust.twitter.data.Tweet;
import sa.edu.kaust.twitter.data.TweetBlockReader;
import sa.edu.kaust.utils.UrlExtractionAndNormalization;


public class TestAnalyzer {

	public static void main(String[] args) throws IOException {
		//String test = "That's right my united department got name dropped on top gear. Hows your united looking now people? #topgear";
		//String test = "\"Bill Kristol's odd omission\" via Pajamasmedia http://bit.ly/fcOMVy Kristol=Useful Idiot of the Left? Corrupt? Jealous? @glennbeck #tcot";
		String test = "@rtyrty34534@rtyrtytr ####tcot#hfgh";
		System.out.println(test);
		//System.out.println(UrlExtractionAndNormalization.URLExtractAndNormalize(test, false));
		//System.out.println(TweetTokenizer.detectCommentWithRetweet(TweetTokenizer.getTokenStream(test)));
		System.out.println(TweetTokenizer.getStringStream(TweetTokenizer.getTokenStream(test)));
		if(true) return;
		String filePath = "data/sequence/part-r-00004";
		//TweetTokenizer tweetTokenizer = new TweetTokenizer();
		TweetBlockReader tweetSeqFileReader = new TweetBlockReader(new Path(filePath));
		Tweet tweet = tweetSeqFileReader.next();
		int i = 0;
		while(tweet !=null){
			i++;
			System.out.println(i+"-"+tweet);
			System.out.println(TweetTokenizer.getStringStream(TweetTokenizer.getTokenStream(tweet.getMessage())));
			//System.out.println(UrlExtractionAndNormalization.extractAndNormalizeURL(tweet.getMessage(), false));
			tweet = tweetSeqFileReader.next();
		}
		tweetSeqFileReader.close();
	}

}
