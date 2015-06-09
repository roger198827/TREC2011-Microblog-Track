package sa.edu.kaust.twitter.tokenize;

import ivory.tokenize.GalagoTokenizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

import edu.umd.cloud9.io.pair.PairOfStringInt;


import sa.edu.kaust.twitter.data.Tweet;
import sa.edu.kaust.twitter.tokenize.TweetToken.TweetTokenType;
import sa.edu.kaust.utils.UrlExtractionAndNormalization;

public class TweetTokenizer {

	//private static String delimiters = "[\\s+();+\\[\\]\\{\\}*,\\-!+&%$\"=.`:?^\\']"; //s+ is regex for space, add \\' to handle Nando's
	private static String delimiters = "[\\s+();+\\[\\]\\{\\}*,\\-!+&%$\"=.`:?^]"; //s+ is regex for space, add \\' to handle Nando's
	static boolean toLowerCase = true;
	static boolean stem = true;
	static final int minTokenLength = 3; 
	static GalagoTokenizer galagoTokenizer = new GalagoTokenizer();
	
	public static ArrayList<TweetToken> getTokenStream(String tweet){
		ArrayList<TweetToken> stream = new ArrayList<TweetToken>();
		//1- extract URL
		PairOfStringInt url = UrlExtractionAndNormalization.extractURL(tweet);

		if(url != null){//URL exists
			int start = url.getRightElement();
			//if(start > 0)
			stream = getTokenStreamWithoutURL(tweet.substring(0, start));
			stream.add(new TweetToken(start, url.getLeftElement(),TweetTokenType.URL));
			start += url.getLeftElement().length();
			if(start<tweet.length()-1){
				ArrayList<TweetToken> tmp = getTokenStream(tweet.substring(start));
				for(TweetToken token : tmp){
					token.startPos+=start;
					stream.add(token);
				}
				tmp = null;
			}
			return stream;
		}
		stream = getTokenStreamWithoutURL(tweet);
		return stream; 
	}

	static boolean isNumber(String s){
		for(char c: s.toCharArray()){
			if (!Character.isDigit(c)) return false;
		}
		return true;
	}
	private static ArrayList<TweetToken> getTokenStreamWithoutURL(String tweet){
		
		ArrayList<TweetToken> stream = new ArrayList<TweetToken>();
		
		String[] tokens = tweet.split(delimiters);
		TweetTokenType type;
		int curPos = 0;
		int l;
		String[] stems = null;
		for(String token: tokens){
			if(token.equals("")) continue;
			/*if (token.startsWith("\'"))
				token = token.substring(1);
			if(token.endsWith("\'"))// I remove the 'else' here to handle if word start and end with \'
				token = token.substring(0, token.length()-1);
			*/
			type = TweetTokenType.OTHER;
			if(token.equals("RT")){ //retweet 
				type = TweetTokenType.RT;
				token ="";
				stream.add(new TweetToken(curPos, token, type));
				curPos+=2;
				continue;
			}
			if(token.startsWith("@")){ // mention 
				type = TweetTokenType.MENTION;
				//if(token.endsWith(":")) token = token.substring(0, token.length()-1);
				curPos = tweet.indexOf(token, curPos);
				token = token.substring(1);
				l = token.length();
				token = getCleanUserName(token);
				if(token == null){
					curPos += l + 1;
					continue;
				}
				if(toLowerCase) token = token.toLowerCase();
				if(token.length()>=minTokenLength && !isNumber(token)) stream.add(new TweetToken(curPos, token, type));//add conditional if length of token>=2
				curPos += token.length() + 1;
				//TODO: parse the mention to remove any trailing bad chars
			}else if(token.startsWith("#")){  // hash tag
				type = TweetTokenType.HASHTAG;
				curPos = tweet.indexOf(token, curPos);
				int k = 0;
				do { 
					k++;
					token = token.substring(1);
				}while(token.startsWith("#"));
				if(toLowerCase) token = token.toLowerCase();
				if(token.length()>=minTokenLength && !isNumber(token)) stream.add(new TweetToken(curPos, token, type));//add conditional if length of token>=2
				curPos += token.length() + k;
			}
			else{// other
				curPos = tweet.indexOf(token, curPos);
				if(toLowerCase) token = token.toLowerCase();
				if(stem){
					stems = galagoTokenizer.processContent(token);
					if(stems!=null && stems.length > 0) token = stems[0];
					else{
						curPos += token.length();
						continue;
					}
				}
				if(token.length()>=minTokenLength && !isNumber(token)) stream.add(new TweetToken(curPos, token, type));//add conditional if length of token>=2
				curPos += token.length();
			}
			
		}
		return stream; 
	}

	public static String getStringStream(ArrayList<TweetToken> stream){
		StringBuffer sb= new StringBuffer();
		//if(stream.size() == 0) return "";
		//sb.append(stream.get(0));
		for(int i = 0; i < stream.size(); i++) sb.append(stream.get(i));
		return sb.toString();
	}

	public static boolean isRetweet(int startToken, ArrayList<TweetToken> tokens){
		if(tokens.size() - startToken<2) return false;
		if(tokens.get(startToken).type == TweetTokenType.RT && tokens.get(startToken+1).type == TweetTokenType.MENTION)
			return true;
		return false;
	}

	public static boolean isReply(ArrayList<TweetToken> tokens){
		if(tokens.size()<1) return false;
		if(tokens.get(0).type == TweetTokenType.MENTION)
			return true;
		return false;
	}

	public static Set<String> getReceivers(ArrayList<TweetToken> tokens){
		Set<String> mentions = new HashSet<String>();
		//String cleanUserName;
		for(TweetToken token : tokens){
			if(token.type != TweetTokenType.MENTION) break;
			/*cleanUserName = getCleanUserName(token.text);			
			if(cleanUserName!=null)
					mentions.add(cleanUserName.toLowerCase());*/
			mentions.add(token.text);
		}
		return mentions;
	}

	public static Set<String> getMentionsNotReceivers(ArrayList<TweetToken> tokens){
		Set<String> mentions = new HashSet<String>();
		boolean receiversEnded = false;
		//String cleanUserName;
		for(TweetToken token : tokens){
			if(!receiversEnded){
				if(token.type != TweetTokenType.MENTION) receiversEnded = true;
				else continue;
			}
			else if(token.type == TweetTokenType.MENTION)
			{
				/*cleanUserName = getCleanUserName(token.text);
				if(cleanUserName!=null)
					mentions.add(cleanUserName.toLowerCase());*/
				mentions.add(token.text);
			}
		}
		return mentions;
	}
	
	public static Set<String> getMentionsIncludingReceivers(ArrayList<TweetToken> tokens){
		Set<String> mentions = new HashSet<String>();
		//String cleanUserName;
		for(TweetToken token : tokens){
			if(token.type == TweetTokenType.RT) break;
			if(token.type == TweetTokenType.MENTION)
			{
				/*cleanUserName = getCleanUserName(token.text);
				if(cleanUserName!=null)
					mentions.add(cleanUserName.toLowerCase());*/
				mentions.add(token.text);
			}
		}
		return mentions;
	}
	
	public static boolean detectRetweet(Tweet tweet, Text reTweetContent, ArrayList<TweetToken> tokens){
		String msg = tweet.getMessage();
		if(tweet.getCode() == 302 || tweet.getCode() == 200){
			//ArrayList<TweetToken> tokens = TweetTokenizer.getTokenStream(msg);
			boolean retweet = false;
			int curToken = 0;
			while(isRetweet(curToken, tokens)){
				curToken += 2;
				if(curToken == tokens.size()) break;
				//msg = msg.substring(tokens.get(curToken).startPos).trim();
				retweet = true;
			}
			if(curToken == tokens.size()) return false;
			if(tweet.getCode() == 302 || retweet){
				reTweetContent.set(msg.substring(tokens.get(curToken).startPos));
				return true;
			}
		}
		return false;
	}

	public static int detectCommentWithRetweet(ArrayList<TweetToken> tokens){
		// if "Comment with Retweet", return position of first token of the "Retweet" part (which is "RT")
		// if not, return -1
		//System.out.println("token: "+tokens.get(1).toString());
		boolean commentFound = false;
		int curToken = 0;
		while(curToken<tokens.size() && !isRetweet(curToken, tokens)){
			curToken++;
			commentFound = true;
		}
		if(!commentFound) return -1;
		if(!isRetweet(curToken, tokens)) return -1;
		System.out.println(curToken);
		return curToken;
	}
	
	public static String getCleanUserName(String userNameCandidate)
	{			
		int j=0;
		boolean found=false;	
		if (userNameCandidate.length()<=1)	return null;
		while(!found && j<userNameCandidate.length()){		
			if(!Character.isLetter(userNameCandidate.charAt(j)) && !Character.isDigit(userNameCandidate.charAt(j)) && !(userNameCandidate.charAt(j)=='_')){			
				found=true;
			}
			else j++;			
		}	
		return userNameCandidate.substring(0,j);
	}
}
