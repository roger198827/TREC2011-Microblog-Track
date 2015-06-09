package sa.edu.kaust.twitter.preprocess;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.FSProperty;

import sa.edu.kaust.twitter.dbconnection.Db;
import sa.edu.kaust.twitter.dbconnection.NewDb;
import sa.edu.kaust.twitter.index.BuildPostingsForwardIndex;
import sa.edu.kaust.twitter.index.IndexTweets;
import sa.edu.kaust.twitter.index.PostingsForwardIndex;
import sa.edu.kaust.twitter.preprocess.hashtag.GetHashtagRepresentation;
import sa.edu.kaust.twitter.preprocess.spam.DetectSpamUsers;
import sa.edu.kaust.twitter.preprocess.spam.RemoveTweetsOfSpamUsers;
import sa.edu.kaust.twitter.preprocess.url.CreateURLCSV;
import sa.edu.kaust.twitter.preprocess.url.CreateURLTIDCSV;
import sa.edu.kaust.twitter.preprocess.url.GetUrlRepresentation;
import sa.edu.kaust.twitter.preprocess.user.GetUserTermRepresentation;
import sa.edu.kaust.twitter.preprocess.user.IndexUsers;
import sa.edu.kaust.twitter.preprocess.user.UserIndex;
import sa.edu.kaust.twitter.retrieval.QueryParser;
import sa.edu.kaust.twitter.retrieval.score.TweetScoringFunction;

public class SetUpDb {

	private static final Logger sLogger = Logger.getLogger(SetUpDb.class);
	static {
		sLogger.setLevel(Level.INFO);
	}

	public static final String LOG_FILE_NAME = "driverLogAll.txt";
	public static final String SPAM_USERS_DIRECTORY = "spamUserList";
	public static final String NON_SPAM_DIRECTORY = "non-spam";
	public static final String TWEET_CSV_DIRECTORY = "tweet";
	public static final String REPLY_CSV_DIRECTORY = "reply";
	public static final String MENTION_CSV_DIRECTORY = "mention";
	public static final String RETWEET_CSV_DIRECTORY = "retweet";
	public static final String URL_CSV_DIRECTORY = "url";
	public static final String URLTID_CSV_DIRECTORY = "urltid";
	public static final String URL_REPRESENTATION_DIRECTORY = "url-representation";
	public static final String NUM_OF_TWEETS_FILE = "num-of-tweets.property";
	
	static int spamUrlThreshold = 3;
	static int spamUserThreshold = 3;
	static int strictspamURLThreshold = 5;
	static int numReducers = 24;
	
	public static void main(String[] args) {
		/*Usage 
		 * 1. username
		 * 2. password
		 * 3. input path
		 * 4. origoutputRoot
		 * 5. endID
		 * 6. indication of removing spam-user tweets(1 for using this feature)*/		//output?

		String dbUserName = args[0];
		String dbPassword = args[1];
		String originalEnglishOnlyTweetsPath=args[2];
		String origOutputRoot = args [3] + "/";
		String outputRoot = origOutputRoot;
		String endID = args[4];
		//String resultsFileName = endID+".txt";
		
		try {
			String startID= "0";
			String cleanTweetsPath;
			long startTime, endTime ;

			long startRun, endRun;
			//long startQuery, endQuery;
			startRun = System.currentTimeMillis();			
			int i = 0;
			int numOfNonSpamTweets = 0;
			int numOfUsers = 0;
			Configuration conf = new Configuration();
			
				NewDb dbConnection = new NewDb("localhost:3306", dbUserName, dbPassword);
				//============================removing spam=================================
				cleanTweetsPath = originalEnglishOnlyTweetsPath;
				/*startTime = System.currentTimeMillis();
				DetectSpamUsers.detectSpamUsers(args[2], outputRoot+SPAM_USERS_DIRECTORY, Long.parseLong(startID), Long.parseLong(endID), spamUrlThreshold, spamUserThreshold, strictspamURLThreshold); 
				numOfNonSpamTweets = RemoveTweetsOfSpamUsers.removeTweetsOfSpamUsers(originalEnglishOnlyTweetsPath, outputRoot+NON_SPAM_DIRECTORY ,numReducers, 
						outputRoot+SPAM_USERS_DIRECTORY+"//part-r-00000", Long.parseLong(startID), Long.parseLong(endID), outputRoot+NUM_OF_TWEETS_FILE);
				cleanTweetsPath=outputRoot+NON_SPAM_DIRECTORY;
				endTime = System.currentTimeMillis();					
				sLogger.info("Removing spam tweets took " + (endTime - startTime) + " milliseconds");*/
				//============================delete data from database=====================
				startTime = System.currentTimeMillis();
				dbConnection.deleteAllRecords();
				dbConnection.CreateTableStructure();
				endTime = System.currentTimeMillis();					
				sLogger.info("Database Tables deletion took " + (endTime - startTime) + " milliseconds");	
				//============================create CSV====================================
				startTime = System.currentTimeMillis();
				CreateTweetCSV.createTweetCSV(cleanTweetsPath,outputRoot+TWEET_CSV_DIRECTORY,1,startID,endID);
				CreateReplyCSV.createReplyCSV(cleanTweetsPath,outputRoot+REPLY_CSV_DIRECTORY,startID,endID);
				CreateMentionCSV.createMentionCSV(cleanTweetsPath,outputRoot+MENTION_CSV_DIRECTORY,startID,endID);
				DetectRetweets.runDetectRetweet(cleanTweetsPath,outputRoot+RETWEET_CSV_DIRECTORY,startID,endID);
				CreateURLCSV.createURLCSV(cleanTweetsPath,outputRoot+URL_CSV_DIRECTORY,startID, endID);
				CreateURLTIDCSV.RunCreateURLTIDCSV(cleanTweetsPath,outputRoot+URLTID_CSV_DIRECTORY, startID, endID);

				endTime = System.currentTimeMillis();					
				sLogger.info("Creation of CSV files took " + (endTime - startTime) + " milliseconds");		
				//====================================Load To DB=========================================
				startTime = System.currentTimeMillis();
				//FileSystem
				File[] listOfFiles;
				File folderPath = new File(TWEET_CSV_DIRECTORY);
				listOfFiles = folderPath.listFiles();
				for (int h=0;h<listOfFiles.length;h++)
				{
					if (listOfFiles[h].isFile()) 
						if (!listOfFiles[h].getName().startsWith(".")) 
							dbConnection.LoadDataToDb("tweet",TWEET_CSV_DIRECTORY+"//"+listOfFiles[h].getName());
						else
							System.out.println(listOfFiles[h].getName()+" not a file");
				}										
				endTime = System.currentTimeMillis();
				sLogger.info("Tweet Loading Operation took " + (endTime - startTime) + " milliseconds \n");
				startTime = System.currentTimeMillis();
				dbConnection.LoadDataToDb("reply", REPLY_CSV_DIRECTORY+"//part-r-00000");
				endTime = System.currentTimeMillis();
				sLogger.info("Reply Loading Operation took " + (endTime - startTime) + " milliseconds \n");
				startTime = System.currentTimeMillis();
				dbConnection.LoadDataToDb("mention", MENTION_CSV_DIRECTORY+"//part-r-00000");
				dbConnection.LoadDataToDb("retweet", RETWEET_CSV_DIRECTORY+"//part-r-00000");
				dbConnection.LoadDataToDb("url", URL_CSV_DIRECTORY+"//part-r-00000");
				dbConnection.LoadDataToDb("urltid", URLTID_CSV_DIRECTORY+"//part-r-00000");
				endTime = System.currentTimeMillis();
				sLogger.info("Mention, Retweet, URL, URLTID loading Operation took " + (endTime - startTime) + " milliseconds \n");
				
			endRun = System.currentTimeMillis();
			sLogger.info(">> The entire DB SetUp took " + (endRun - startRun) + " milliseconds \n");
			//out.close();
			dbConnection.DbConnect.DestroyConnection();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} /*catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}//end of function main
}
