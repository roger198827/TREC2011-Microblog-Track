package sa.edu.kaust.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.StringTokenizer;

import sa.edu.kaust.twitter.data.Tweet;

public class Util {

	public static boolean isreply(String tweet)
	{
		return tweet.startsWith("@");
	}
	
	public static LinkedList<String> ReplyToWhom(String Tweet)
	{
		LinkedList<String> ReplyTo = new LinkedList<String>();
		int index, StartIndex, EndOfUserName;
		boolean MessageStart;
		String FriendName;
		if (isreply(Tweet))
		{
			index=0;
			MessageStart=false;
			while(MessageStart==false)
			{
				StartIndex= Tweet.indexOf("@", index);
				//System.out.println("start= "+StartIndex);
				EndOfUserName = getEndOfUserName(Tweet.substring(StartIndex+1));
				if (EndOfUserName<0)//if none of the above, assume the name in the end of message
				{
					EndOfUserName= Tweet.length();						
					if(EndOfUserName-1!=StartIndex)//if not only @ in the end
					{
						FriendName = Tweet.substring(StartIndex+1,EndOfUserName);
						ReplyTo.add(FriendName);
						MessageStart = true;
						//System.out.println("friend0= "+FriendName+" tweet= "+Tweet);
					}else
						FriendName ="";
				}else
				{
					ReplyTo.add(Tweet.substring(StartIndex+1, EndOfUserName+StartIndex+1));
					//System.out.println("friend= "+FriendName);
					//System.out.println("replyto= "+Tweet.substring(StartIndex+1, EndOfUserName+StartIndex+1)+" tweet: "+Tweet);
					if(EndOfUserName+StartIndex+3<Tweet.length())
					{
							if (!Tweet.substring(EndOfUserName+StartIndex+2, EndOfUserName+StartIndex+3).equalsIgnoreCase("@"))
					
						{
							MessageStart = true;
						}
						else
						{					
							index = EndOfUserName+StartIndex;
						}
					}else
						MessageStart = true;
				}
			}
		}else
			System.out.println("object of toWhom isn't a reply");
		return ReplyTo;
	}
	
	public static boolean isretweet(String tweet)
	{
		return tweet.contains("RT @");
	}
	
	public static boolean hasUrl(String message)
	{
		return message.contains("http://");
	}
	
	public static boolean ContainMention(String Tweet)
	{
		boolean contain=false;
		if (isreply(Tweet))
		{
			int last = GetLastIndexOfReplyTo(Tweet);
			if(Tweet.substring(last).contains("@"))
				contain=true;
		}else if(Tweet.contains("@"))
			contain=true;
		return contain;
	}
	
	public static int GetLastIndexOfReplyTo(String Tweet)
	{
		int last=0;//return zero if it is actually not a reply
		if (isreply(Tweet))//just in case it is not a reply
		{
			LinkedList<String> ReplyTo = ReplyToWhom(Tweet);
			//detecting last index to start searching for
			last=0;
			for (int k=0;k<ReplyTo.size();k++)
			{
				//System.out.println("length of word"+k+": "+ReplyTo.get(k).length());
				last+=1;//the '@'
				last+=ReplyTo.get(k).length();//length of word
				last+=1;//space
			}
			ReplyTo.clear();
		}
	
		return last-1;
	}
	
	public static Set<String> getMentions(String TweetMessage)
	{
		int index=0;
		Set<String> TheMentionList = new HashSet<String>();
		System.out.println("tweet: "+TweetMessage);
		while(TweetMessage.indexOf("@", index)>=0)
		{
				int FirstOccurence= TweetMessage.indexOf("@", index);				
				int EndNameOccurence= Util.getEndOfUserName(TweetMessage.substring(FirstOccurence+1));
				String FriendName="";
				if(EndNameOccurence>0 && FirstOccurence>=0)
				{										
					FriendName = TweetMessage.substring(FirstOccurence+1,EndNameOccurence+FirstOccurence+1); //get the username and remove space(if any)
					
				}
				else if(EndNameOccurence==0)//just @ no username after
				{
					FriendName="";
				}else
				{
					if (EndNameOccurence<0)//if none of the above, assume the name in the end of message
					{
						EndNameOccurence= TweetMessage.length();						
						if(EndNameOccurence-1!=FirstOccurence)
						{
							FriendName = TweetMessage.substring(FirstOccurence+1,EndNameOccurence);
							TheMentionList.add(FriendName);
							//System.out.println("friendGetMention= "+FriendName);
						}else
							FriendName ="";
						break;
					}
					else 
					{
							System.out.println("Error in getMentions");
							System.out.println("tweet= "+TweetMessage);
					}
				}						
				if(!FriendName.equalsIgnoreCase(""))
					{
						TheMentionList.add(FriendName);
						//System.out.println("friend= "+FriendName);
					}
				index = EndNameOccurence+FirstOccurence+1;				
		}
		return TheMentionList;
	}
	
	public static LinkedList<String[]> connectUsers(Tweet TheTweet)//output used for constructing user graph
	{
		String[] userpair= new String[2];
		LinkedList<String[]> connection= new LinkedList<String[]>();
		
		if (Util.isretweet(TheTweet.getMessage()))
		{
			userpair[0]=TheTweet.getUserName();
			int FirstOccurence= TheTweet.getMessage().indexOf("RT @");
			int EndNameOccurence= getEndOfUserName(TheTweet.getMessage().substring(FirstOccurence+4));
			String FriendName="";
			if(EndNameOccurence>0 && FirstOccurence>0)
			{				
				FriendName = TheTweet.getMessage().substring(FirstOccurence+4,EndNameOccurence+FirstOccurence+4); //get the username and remove space(if any)
			}
			else if(EndNameOccurence==0)//just @ no username after
			{
				FriendName="";
			}
			else 
			{
				if (EndNameOccurence<0)//if none of the above, assume the name in the end of message
				{
					EndNameOccurence= TheTweet.getMessage().length();
					if(EndNameOccurence-1!=FirstOccurence)
						FriendName = TheTweet.getMessage().substring(FirstOccurence+4,EndNameOccurence);
					else
						FriendName="";
				}
				else 
				{
					System.out.println("Error in Connect Users");
					System.out.println("tweet= "+TheTweet.getMessage());
				}
			}
					
			userpair[1]=FriendName;//System.out.println("connected with:= "+userpair[1]); 			
			if(!FriendName.equalsIgnoreCase(""))connection.add(userpair);
		}else
		{
			Set<String> MentionedList = Util.getMentions(TheTweet.getMessage());
			Iterator<String> MentionedListIterator = MentionedList.iterator();
			while (MentionedListIterator.hasNext())
			{
				userpair= new String[2];
				userpair[0]=TheTweet.getUserName();
				userpair[1]=MentionedListIterator.next().toString();				
				connection.add(userpair);
				//System.out.println("connected with:= "+userpair[1]);
			}
			MentionedList.clear();
		}		
		
		return connection;
	}
	
	public static int getEndOfUserName(String MessageFromTheStartOfName)
	{
		int spaceIndex= MessageFromTheStartOfName.indexOf(' ');
		String check="";
		if (spaceIndex<0)
			check=MessageFromTheStartOfName;
		else
			check=MessageFromTheStartOfName.substring(0,spaceIndex);
		
		int j=0;
		boolean found=false;
		//System.out.println("check= "+check+" s: "+check.length());
		if (check.length()<=1)
		{
			return -1;
		}
		while(found==false && j<check.length())
		{
			//System.out.println("j= "+j);
			if(!Character.isLetter(check.charAt(j)) && !Character.isDigit(check.charAt(j)) && !check.substring(j, j+1).equalsIgnoreCase("_"))
			{
				//System.out.println("found= "+check.charAt(j));
				found=true;
			}
			else
				j++;			
		}
		//System.out.println("j= "+j);
		return j;
	}
	
	public static String convertTimeStamp(String timeStamp)
	{
		String trueTimeStamp="2011-";
		StringTokenizer tokens=new StringTokenizer(timeStamp," ");
		String[] TweetLine = new String[6];
        
        for(int j=0;j<6;j++)
        {
               TweetLine[j]=tokens.nextToken();	                 
        }// end of while        
        
        if (TweetLine[1].equalsIgnoreCase("jan"))
        	trueTimeStamp+="01-";
        trueTimeStamp+=TweetLine[2]+" "+TweetLine[3];
		return trueTimeStamp.trim();
	}
}
