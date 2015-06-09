package sa.edu.kaust.utils;

import java.io.*;
import java.util.LinkedList;
import java.util.StringTokenizer;

import sa.edu.kaust.twitter.data.Tweet;

public class TweetFile {
	
	public LinkedList<Tweet> TheTweetList;
	
	public LinkedList<Tweet> read(String FileName)
	{
		LinkedList<Tweet> TheTweetList = new LinkedList<Tweet>();
		LinkedList<String> TweetLine;
		Tweet newTweet;
		
		File file = new File(FileName);
	    FileInputStream fis = null;
	    BufferedInputStream bis = null;
	    DataInputStream dis = null;
	    String ALine, word;
	    
	    try {
	        fis = new FileInputStream(file);
	        // Here BufferedInputStream is added for fast reading.
	        bis = new BufferedInputStream(fis);
	        dis = new DataInputStream(bis);

	        while (dis.available() != 0) {

	           ALine=dis.readLine();
	           StringTokenizer st = new StringTokenizer(ALine);
	           TweetLine = new LinkedList<String>();
	           
	           while (st.hasMoreTokens())
	           {
	                  word = st.nextToken();
	                  TweetLine.add(word);	                 
	           }// end of while	           	           
	           
	           if (TweetLine.size()>5)
	           {
		           String TimeStamp= TweetLine.get(3)+" "+TweetLine.get(4)+" "+TweetLine.get(5)+" "+TweetLine.get(6)+" "+TweetLine.get(7)+" "+TweetLine.get(8);
		           String Message="";
		           for(int b=9;b<TweetLine.size();b++)
		           {
		        	   Message= Message +" "+ TweetLine.get(b);
		           }		           
		           newTweet = new Tweet(Long.parseLong(TweetLine.get(0)),TweetLine.get(1),Integer.parseInt(TweetLine.get(2)), TimeStamp, Message);
	           }
	           else
	           {
	        	   newTweet = new Tweet(Long.parseLong(TweetLine.get(0)),TweetLine.get(1),Integer.parseInt(TweetLine.get(2)), "","<no message>");
	           }
	           TheTweetList.add(newTweet);
	           
	        }
	        fis.close();
	        bis.close();
	        dis.close();
	    }
	    catch (FileNotFoundException e) {
	      e.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	    this.TheTweetList = TheTweetList;
	    
		return TheTweetList;
	}
	
	/*void GetConversationInFile(LinkedList<Tweet> TheTweetList)
	{
		
	}*/
	
	void ConnectUsersInFile(String OutputFileName)
	{
		LinkedList<String[]> UserConnection = new LinkedList<String[]>();
		
		FileWriter fstream;
		try {
			fstream = new FileWriter(OutputFileName);
			BufferedWriter out = new BufferedWriter(fstream);
		
	    
			for(int i=0;i<TheTweetList.size();i++)
			{
				//System.out.println(TheTweetList.get(i).getMessage());
				UserConnection= Util.connectUsers(TheTweetList.get(i));
				for (int j=0;j<UserConnection.size();j++)
				{
					out.write(UserConnection.get(j)[0]+" "+UserConnection.get(j)[1]+"\n");
				}
				UserConnection = new LinkedList<String[]>();
			}
			 out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
