package sa.edu.kaust.twitter.tokenize;

import java.awt.List;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.umd.cloud9.io.pair.PairOfStrings;

import sa.edu.kaust.twitter.data.Tweet;
import sa.edu.kaust.twitter.data.TweetCollectionReader;
import sa.edu.kaust.twitter.dbconnection.Db;
import sa.edu.kaust.twitter.dbconnection.MySqlConnection;
import sa.edu.kaust.utils.Util;
public class TestTokenFunctions {
	
	//read All files
	
	//temporary test
	public static void main(String[] args) {
		
		//String text="@lely @tika @rahma @restya tralala dubi dubidu syalala";
		/*Path direct= new Path("D:\\KAUST\\SUMMER\\hadoop-0.20.2\\twitter\\input");
		Tweet MyTweet;	
		int length;
		TweetCollectionReader TweetSequence;
		try {
		TweetSequence = new TweetCollectionReader(direct);
		MyTweet= TweetSequence.next();
		do
		{								
				System.out.println("tweet : "+MyTweet.getMessage());
				ArrayList<TweetToken> tokens= TweetTokenizer.getTokenStream(MyTweet.getMessage());
			if(TweetTokenizer.isReply(tokens))
			{		
				for (int y=0;y<TweetTokenizer.getReceivers(tokens).size();y++)
				{
					System.out.println("replyto : "+TweetTokenizer.getReceivers(tokens).get(y));
				}
			}
						
			//String text2="@sara @iman how is it in German??? I wanna go there someday  @huda @rana @huda";
			//ArrayList<TweetToken> tokens= TweetTokenizer.getTokenStream(text2);
			Text myText= new Text(MyTweet.getMessage());
			if(TweetTokenizer.detectRetweet(MyTweet, myText, tokens))
			{
				System.out.println("This is a retweet");
				System.out.println(myText.toString());
				ArrayList<TweetToken> tokens2= TweetTokenizer.getTokenStream(myText.toString());
				Set<String> myMention;
			    if(TweetTokenizer.detectCommentWithRetweet(tokens2)>0)
			    {
			    	System.out.println("this is a retweet with comment with "+TweetTokenizer.detectCommentWithRetweet(tokens2));
			    	
			    	/*myMention= TweetTokenizer.getMentionsNotReceivers(comment);
			    	length= myMention.size();
			    	if (myMention.size()>0)
					{
						System.out.println("contain mention :"+length);
						Iterator<String> MentionedListIterator = myMention.iterator();
						while (MentionedListIterator.hasNext())
						{
							System.out.println(MentionedListIterator.next().toString());
						}
					}
					else
					System.out.println("doesn't contain mention");
					//System.out.println("contain mention");
			    }
			    else
			    {
					myMention= TweetTokenizer.getMentionsNotReceivers(tokens2);
					length= myMention.size();
					if (myMention.size()>0)
						{
							System.out.println("contain mention :"+length);
							Iterator<String> MentionedListIterator = myMention.iterator();
							while (MentionedListIterator.hasNext())
							{
								System.out.println(MentionedListIterator.next().toString());
							}
						}
						else
						System.out.println("doesn't contain mention");
						//System.out.println("contain mention");
			    }
			}		
			MyTweet= TweetSequence.next();	
		}while(MyTweet!=null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		String text3 = "sophisticated blablabla";
		ArrayList<TweetToken> tokens= TweetTokenizer.getTokenStream(text3);
		int f= TweetTokenizer.detectCommentWithRetweet(tokens);
		if (f>0)
			System.out.println("this tweet is retweet with comment. Tweet : "+text3);
		else
			System.out.println("this tweet isn't a retweet with comment");
		
		//======================test of get receiver===========================
		/*String text="@* @) @@ this is what I need to do";
		ArrayList<TweetToken> tokens= TweetTokenizer.getTokenStream(text);
		ArrayList<String> receiver= TweetTokenizer.getReceivers(tokens);
		System.out.println(receiver.size());
		for (int y=0;y<receiver.size();y++)
		{
			System.out.println(receiver.get(y));
		}*/
		
	}

}
