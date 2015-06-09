package sa.edu.kaust.twitter.dbconnection;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import edu.umd.cloud9.util.map.HMapKF;
import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKI;

public class NewDb {

	public MySqlConnection DbConnect ;
	
	public NewDb(String ipServer,String username,String password)
	{
		try {
			DbConnect = new MySqlConnection();
			DbConnect.GetConnection(ipServer, username, password);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteAllRecords()
	{
		try {
			DbConnect.ExecuteNonQuery("drop table if exists reply, mention, retweet, urltid, url, tweet");
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public float getUserPopularity(String username, float alpha, float beta, float gamma, long endID)
	{
		int value=0;
		float score=0;
		
		HMapKF<String> follower=new HMapKF<String>();
		HMapKI<String> retweeter=new HMapKI<String>();
		HMapKI<String> replier=new HMapKI<String>();
		HMapKI<String> mentioner=new HMapKI<String>();
		
		getCountUsersRetweetingMe(username, retweeter, endID);
		getCountUsersReplyingToMe(username, replier, endID);
		getCountUsersMentioningMe(username, mentioner, endID);
		
		for (MapKI.Entry<String> user:retweeter.entrySet())//merging retweet set to follower
		{
			value =user.getValue();
			if (value>=2)//threshold for retweet=2
			{
				if(follower.containsKey(user.getKey()))
				{
					follower.put(user.getKey(), follower.get(user.getKey())+gamma*1);
				}else
				{
					follower.put(user.getKey(), gamma*value);
				}
			}
		}
		/*for (String user:retweeter.keySet())//merging retweet set to follower
		{
			value =retweeter.get(user);
			if (value>=2)//threshold for retweet=2
			{
				if(follower.containsKey(user))
				{
					follower.put(user, follower.get(user)+gamma*value);
				}else
				{
					follower.put(user, gamma*value);
				}
			}
		}*/
		
		for (String user:replier.keySet())//merging reply set to follower
		{
			value =replier.get(user);
			if (value>=1)//threshold for reply=1
			{
				if(follower.containsKey(user))
				{
					follower.put(user, follower.get(user)+alpha*1);
				}else
				{
					follower.put(user, alpha*replier.get(user));
				}
			}
		}
		
		for (String user:mentioner.keySet())//merging mention set to follower
		{
			value =mentioner.get(user);
			if (value>=2)//threshold for mention=2
			{
				if(follower.containsKey(user))
				{
					follower.put(user, follower.get(user)+beta*1);
				}else
				{
					follower.put(user, beta*mentioner.get(user));
				}
			}
		}
		
		//computing the total score
		score=0;
		for (float fol : follower.values()) 
			{
				//System.out.println(fol+": "+follower.get(fol));
				score+= fol;
			}
		follower.clear();
		mentioner.clear();
		replier.clear();
		retweeter.clear();
		follower = null;
		mentioner = null;
		replier = null;
		retweeter = null;
		return score;
	}
	
	
public String getUserName(String tweetID) throws IllegalStateException, SQLException
	{
		String query="select username from tweet where tweetID= "+tweetID;
		String userName="";
		ResultSet rs;
		rs = DbConnect.ExecuteSelect(query);
		rs.next();
		if (rs.getString(1)!=null)
		{				
			userName = rs.getString(1);
		}
		rs.close();
		return userName;
	}
	
	public void CreateTableStructure() throws IllegalStateException, SQLException
	{
		String query="CREATE TABLE IF NOT EXISTS `tweet` (  `TweetID` bigint(20) NOT NULL,  `UserName` varchar(16) DEFAULT NULL,  PRIMARY KEY (`TweetID`),  KEY `UserName` (`UserName`)) ENGINE=MyISAM DEFAULT CHARSET=latin1 ";
		DbConnect.ExecuteNonQuery(query);
		query = "CREATE TABLE IF NOT EXISTS `mention` (  `MentionID` int(10) NOT NULL AUTO_INCREMENT,  `MentionTID` varchar(20) DEFAULT NULL,  `Mentioned` varchar(140) DEFAULT NULL,  PRIMARY KEY (`MentionID`),  KEY `mentiontid_indexonly` (`MentionTID`),  KEY `mentioned-indexonly` (`Mentioned`)) ENGINE=MyISAM AUTO_INCREMENT=1932198 DEFAULT CHARSET=latin1 ";
		DbConnect.ExecuteNonQuery(query);
		//query="CREATE TABLE IF NOT EXISTS `reply` (  `ReplyID` bigint(20) NOT NULL AUTO_INCREMENT,  `ReplyTID` bigint(20) DEFAULT NULL,  `ReplyTo` varchar(140) DEFAULT NULL,  PRIMARY KEY (`ReplyID`),  KEY `replyto_key` (`ReplyTo`),  KEY `replytid_key` (`ReplyTID`)) ENGINE=InnoDB AUTO_INCREMENT=6056483 DEFAULT CHARSET=latin1 ";
		query="CREATE TABLE IF NOT EXISTS `reply` (  `ReplyID` bigint(20) NOT NULL AUTO_INCREMENT,  `ReplyTID` bigint(20) DEFAULT NULL,  `ReplyTo` varchar(140) DEFAULT NULL,  PRIMARY KEY (`ReplyID`),  KEY `replyto_key` (`ReplyTo`),  KEY `replytid_key` (`ReplyTID`)) ENGINE=MyIsam AUTO_INCREMENT=6056483 DEFAULT CHARSET=latin1 ";
		DbConnect.ExecuteNonQuery(query);
		query = "CREATE TABLE IF NOT EXISTS `retweet` (  `retweetid` int(10) NOT NULL AUTO_INCREMENT,  `RetweetTID` varchar(20) DEFAULT NULL,  `OriginalTID` varchar(20) DEFAULT NULL,  PRIMARY KEY (`retweetid`),  KEY `index_key_tid` (`RetweetTID`),  KEY `retweet_originaltid_key` (`OriginalTID`)) ENGINE=MyISAM AUTO_INCREMENT=737269 DEFAULT CHARSET=latin1 ";
		DbConnect.ExecuteNonQuery(query);
		query ="CREATE TABLE IF NOT EXISTS `url` (  `URLID` bigint(20) NOT NULL DEFAULT '0',  `URL` varchar(140) DEFAULT NULL,  PRIMARY KEY (`URLID`)) ENGINE=MyISAM DEFAULT CHARSET=latin1 ";
		DbConnect.ExecuteNonQuery(query);
		query = "CREATE TABLE IF NOT EXISTS `urltid` (  `URLTIDID` int(10) NOT NULL AUTO_INCREMENT,  `URLID` bigint(10) NOT NULL,  `TID` bigint(20) NOT NULL,  PRIMARY KEY (`URLTIDID`),  KEY `FK_URLID` (`URLID`),  KEY `FK_TID_TweetID` (`TID`)) ENGINE=MyISAM AUTO_INCREMENT=1291946 DEFAULT CHARSET=latin1 ";
		DbConnect.ExecuteNonQuery(query);
	}
	
	public void LoadDataToDb(String tableName, String filePath)
	{
		String fieldStructure="";
		if(tableName.equalsIgnoreCase("URLTID"))
			fieldStructure="(TID, URLID)";
		else if (tableName.equalsIgnoreCase("retweet"))
			fieldStructure="(originalTID, retweetTID)";
		else if (tableName.equalsIgnoreCase("reply"))
			fieldStructure="(ReplyTID, ReplyTo)";
		else if (tableName.equalsIgnoreCase("mention"))
			fieldStructure="(MentionTID, Mentioned)";
		else if (tableName.equalsIgnoreCase("tweet"))
			fieldStructure="(TweetID, UserName)";
		//else if(tableName.equalsIgnoreCase(""))
		//filePath.replace("\\","\\\");
		//System.out.println("filepath: "+filePath);
		String query = "load data local infile '"+filePath+"' into table "+tableName+"\n"+
						"fields terminated by '\t' \n"+
						"lines terminated by '\n' \n"+
						fieldStructure;
		//System.out.println(query);
		try {
			DbConnect.ExecuteNonQuery(query);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int GetLastID(String TableName)
	{
		int MaxID = 0;
		ResultSet rs;
		try {
			rs = DbConnect.ExecuteSelect("select max("+TableName+"ID) from "+TableName);
			rs.next();
			if (rs.getString(1)!=null)
			{				
				MaxID = rs.getInt(1);
			}else
				MaxID=0;
			rs.close();
		} catch (IllegalStateException e) {			
			e.printStackTrace();
		} catch (SQLException e) {			
			e.printStackTrace();
		}
		//System.out.println("maxID= "+MaxID);
		return MaxID;
	}
	/*============A User Statistics=================*/
	public int GetCountMyTweetBeRetweeted(String Me, long endID)
	{
		long tweetid=0;	
		int count=0;
		//String query="select count(distinct originaltid) as numOriginaltid from retweet where author='"+Me+"'";
		String query="select tweetid from tweet where username='"+Me+"' and tweetid<="+endID;
		//String query="select count(originaltid) from tweet,retweet where tweetid=originaltid and username='"+Me+"'";
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			while(rs.next())
			{
				tweetid= rs.getLong(1);
				String query2="select count(originaltid) from retweet where originaltid='"+tweetid+"' and retweettid<="+ endID;
				ResultSet rs2= DbConnect.ExecuteSelect(query2);
				rs2.next();
				count+=rs2.getInt(1);
				rs2.close();
				//count=rs.getInt(1);
			}
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return count;
	}
	
	public int getCountUsersRetweetingMe(String Me, HMapKI<String> retweeter, long endID)
	{
		//String query="select count(distinct username) from retweet where author='"+Me+"'";
		String query ="select tweetid from tweet where username='"+Me+"' and tweetid<="+endID;
		Long tweetid;
		//Set<String> retweeter= new HashSet();
		//Map<String , Integer> retweeter =new HashMap();
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			while(rs.next())
			{
				tweetid= rs.getLong(1);
				String query2="select username from tweet, retweet where originaltid='"+tweetid+"' and tweetid=retweettid";
				ResultSet rs2=DbConnect.ExecuteSelect(query2);
				while (rs2.next())
					{
						if(retweeter.containsKey(rs2.getString(1)))
							retweeter.put(rs2.getString(1), retweeter.get(rs2.getString(1))+1);
						//retweeter.add(rs2.getString(1));
						else
							retweeter.put(rs2.getString(1), 1);
					}
				rs2.close();
			}
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retweeter.size();		
	}
	
	
	public int GetCountReplyToMe(String Me, long endID)
	{
		int stat=0;		
		String query="select count(replyto) from reply where replyto='"+Me+"' and replytid<="+endID;
		//String query="select count(tweetid) from tweet, reply where replyto=username and replyto='"+Me+"'";
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			rs.next();
			stat= rs.getInt(1);
			//System.out.println(stat);
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stat;
		
	}
	
	public int getCountUsersReplyingToMe(String Me, HMapKI<String> replier, long endID)
	{
		//String query="select count(distinct replierusername) from reply where replyto='"+Me+"'";
		//Map<String , Integer> replier =new HashMap();
		String query="select username from tweet, reply where replytid=tweetid and replyto='"+Me+"' and tweetid<="+endID;
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			while (rs.next())
			{
				if(replier.containsKey(rs.getString(1)))
					replier.put(rs.getString(1), replier.get(rs.getString(1))+1);
				//retweeter.add(rs2.getString(1));
				else
					replier.put(rs.getString(1), 1);
			}
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return replier.size();		
	}
	
	public int GetCountMentionMe(String Me, long endID)
	{
		int stat=0;		
		String query="select count(mentioned) from mention where mentioned='"+Me+"' and mentiontid<="+endID;
		//String query="select count(tweetid) from tweet, reply where replyto=username and replyto='"+Me+"'";
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			rs.next();
			stat= rs.getInt(1);
			//System.out.println(stat);
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stat;		
	}
	
	public int getCountUsersMentioningMe(String Me, HMapKI<String> mentionList, long endID)
	{
		long tweetid=0;	
		//int count=0;
		//Set<String> user=new HashSet<String>();
		//String query="select count(distinct originaltid) as numOriginaltid from retweet where author='"+Me+"'";
		String query="select mentiontid from mention where mentioned='"+Me+"' and mentiontid<="+endID;//get all tid which mention this user
		String user;
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			while(rs.next())
			{
				tweetid= rs.getLong(1);
				String query2="select username from tweet where tweetid='"+tweetid+"'";
				ResultSet rs2= DbConnect.ExecuteSelect(query2);
				
				if(rs2.next() && !rs2.getString(1).equalsIgnoreCase(Me))
					{
						user=rs2.getString(1);
						if(mentionList.containsKey(user))
								mentionList.put(user, mentionList.get(user)+1);
						else
							mentionList.put(rs2.getString(1),1);
					}
				rs2.close();
			}
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return mentionList.size();
	}
	
	/*============Tweet Statistics=================*/
	public int getCountBeingRetweeted(long TweetID, long endID)
	{
		int stat=0;
		
		//String query="select count(username) from retweet where originaltid= '"+TweetID+"'";
		String query="select count(retweettid) from retweet where originaltid= '"+TweetID+"' and retweettid<="+endID;
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			rs.next();
			stat= rs.getInt(1);
			//System.out.println(stat);
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stat;
		
	}
	
	public int GetCountUserRetweetedThis(long TweetID, long endID)
	{
		int stat=0;
		
		//String query="select count(distinct username) from retweet where originaltid= '"+TweetID+"'";
		String query="select count(distinct(username)) number from tweet, retweet where retweettid=tweetid and originaltid='"+TweetID+"' and tweetid<="+endID;
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			rs.next();
			stat= rs.getInt(1);
			//System.out.println(stat);
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stat;
	}
	
	/*public int GetCountUsersMentioned(long TweetID)
	{		
		int stat=0;
		//String query="	select count(mention) from mention where tid='"+TweetID+"'";
		String query="	select count(mentioned) from mention where tid='"+TweetID+"'";
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			rs.next();
			stat= rs.getInt(1);
			System.out.println(stat);
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stat;
	}*/
	
	/*============URL Statistics=================*/
	public int GetTweetCountMentioningThisURL(String URL, long endID)
	{
		int stat=0;
		//String query="select count(TID) from URLuser where URLid= "+URLID;
		String query="select count(tid) from urltid, url where urltid.urlid=url.urlid and url.URL='"+URL+"' and tid<="+endID;
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			rs.next();
			stat= rs.getInt(1);
			//System.out.println(stat);
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stat;
	}
	
	/*public int GetTweetCountMentioningThisURL(long URLID)
	{
		int stat=0;
		//String query="select count(TID) from URLuser where URLid= "+URLID;
		String query="select count(urlid) from urltid where urlid="+URLID;
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			rs.next();
			stat= rs.getInt(1);
			System.out.println(stat);
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stat;
	}*/
	
	public int getTweetURLCount(long tweetid, long endID)
	{
		String urlQuery= "select urlid from urltid where tid="+tweetid;
		int count=0;
		try {
			ResultSet rs=DbConnect.ExecuteSelect(urlQuery);			
			if (!rs.next())
			{
				//System.out.println("This tweet("+tweetid+") doesn't contain any  URL");
			}else
			{
				do
				{
					String urlCountQuery = "select count(urlid) from urltid where urlid="+rs.getLong(1)+" and tid<="+endID;
					ResultSet rs2;
					
						rs2 = DbConnect.ExecuteSelect(urlCountQuery);
						if(rs2.next())count = rs2.getInt(1);
						rs2.close();					
				}while (rs.next());
				rs.close();
			}
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
		return count;
	}
	public int GetUserCountMentioningThisURL(long tweetid, long endID)
	{
		int stat=0;
		Set<String> usernameList=new HashSet();
		String query="select urlid from urltid where tid='"+tweetid+"'";
		try {
			ResultSet rs=DbConnect.ExecuteSelect(query);
			while(rs.next())
			{
				long urlid=rs.getLong(1);
				String query2="select tid from urltid where urlid='"+urlid+"' and tid<="+endID; //only consider the tweet before the endID(queryID)
				ResultSet rs2=DbConnect.ExecuteSelect(query2);
				while(rs2.next())
				{
					long tid=rs2.getLong(1);
					String query3="select username from tweet where tweetid='"+tid+"'";
					ResultSet rs3=DbConnect.ExecuteSelect(query3);
					if(rs3.next())
						{
							String username=rs3.getString(1);
							usernameList.add(username);
						}
					rs3.close();
				}
				rs2.close();
			}
			rs.close();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//get the id of url inside the tweet(if any)
		return usernameList.size();
	}
	
}


