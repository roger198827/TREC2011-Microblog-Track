package sa.edu.kaust.twitter.retrieval;

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
import sa.edu.kaust.twitter.preprocess.CreateMentionCSV;
import sa.edu.kaust.twitter.preprocess.CreateReplyCSV;
import sa.edu.kaust.twitter.preprocess.CreateTweetCSV;
import sa.edu.kaust.twitter.preprocess.DetectRetweets;
import sa.edu.kaust.twitter.preprocess.hashtag.GetHashtagRepresentation;
import sa.edu.kaust.twitter.preprocess.spam.DetectSpamUsers;
import sa.edu.kaust.twitter.preprocess.spam.RemoveTweetsOfSpamUsers;
import sa.edu.kaust.twitter.preprocess.url.CreateURLCSV;
import sa.edu.kaust.twitter.preprocess.url.CreateURLTIDCSV;
import sa.edu.kaust.twitter.preprocess.url.GetUrlRepresentation;
import sa.edu.kaust.twitter.preprocess.user.GetUserTermRepresentation;
import sa.edu.kaust.twitter.preprocess.user.IndexUsers;
import sa.edu.kaust.twitter.preprocess.user.UserIndex;
import sa.edu.kaust.twitter.retrieval.score.TweetScoringFunction;

public class DriverNoDB {

	private static final Logger sLogger = Logger.getLogger(DriverNoDB.class);
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
	public static final String HASHTAG_REPRESENTATION_DIRECTORY = "hashtag-representation";
	public static final String USER_TERM_REPRESENTATION_DIRECTORY = "user-term-representation";
	public static final String USERS_INDEX_DIRECTORY = "users-index";
	public static final String TWEETS_INDEX_DIRECTORY = "tweets-index";
	public static final String TWEETS_INDEX_FORWARD_DIRECTORY = "tweets-index-fwd";
	public static final String NUM_OF_TWEETS_FILE = "num-of-tweets.property";
	public static final String NUM_OF_USERS_FILE = "num-of-users.property";


	static int spamUrlThreshold = 3;
	static int spamUserThreshold = 3;
	static int strictspamURLThreshold = 5;
	static int urlRepTermFrequencyThreshold = 3;
	static int urlRepTopNTerms = 6;
	static int hashtagRepTermFrequencyThreshold = 3;
	static int hashtagRepTopNTerms = 6;
	static int userRepTermFrequencyThreshold = 3;
	static int userRepTopNTerms = 6;
	static int topNTweetsPerQuery = 100;
	static int numReducers = 24;

	public static void main(String[] args) {
		/*Usage 
		 * 1. username
		 * 2. password
		 * 3. input path
		 * 4. Query FilePath
		 * 5. indication of removing spam-user tweets(1 for using this feature)*/		//output?

		String dbUserName = args[0];
		String dbPassword = args[1];
		String originalEnglishOnlyTweetsPath=args[2];
		String origOutputRoot = args [3] + "/";
		String outputRoot = origOutputRoot;
		String queriesFilePath=args[4];//queryPath
		String runID = args[5];
		String resultsFileName = runID+".txt";

		boolean spam = Boolean.parseBoolean(args[6]);
		boolean urlExpand = Boolean.parseBoolean(args[7]);
		boolean hashtagExpand = Boolean.parseBoolean(args[8]);
		boolean rerank = Boolean.parseBoolean(args[9]);
		
		//contentSimilarityWt, userAuthorityWt, recencyWt, tweetPopularityWt,	userPopularityWt, urlPopularityWt
		TweetScoringFunction primaryRankingFn = new TweetScoringFunction(
				1.0f, //contentSimilarityWt
				0.0f, //userAuthorityWt
				0.0f, //recencyWt
				0.0f, //tweetPopularityWt
				0.0f, //userPopularityWt
				0.0f  //urlPopularityWt
		);
		TweetScoringFunction secondaryRankingFn = rerank? new TweetScoringFunction(
				0.1f, //contentSimilarityWt
				0.5f, //userAuthorityWt
				0.0f, //recencyWt
				0.5f, //tweetPopularityWt
				0.5f, //userPopularityWt
				0.5f  //urlPopularityWt
		):null;
				if(!rerank) topNTweetsPerQuery = 50;

				//============================read query====================================
				try {
					String startID= "0";
					String cleanTweetsPath;
					long startTime, endTime ;
					QueryParser.prepareFile(queriesFilePath);		
					ArrayList<Query> queryList= QueryParser.readQueries(queriesFilePath+".xml");

					BufferedWriter resultsOut = new BufferedWriter(new FileWriter(resultsFileName));
					long startRun, endRun;
					long startQuery, endQuery;
					startRun = System.currentTimeMillis();
					NewDb dbConnection = new NewDb("localhost:3306", dbUserName, dbPassword);
					int i = 0;
					int numOfNonSpamTweets = 0;
					int numOfUsers = 0;
					Configuration conf = new Configuration();
					for (Query query : queryList){
						startQuery = System.currentTimeMillis();
						System.out.println(query);
						sLogger.info(i+" :"+query);
						//if (i>0) startID=queryList.get(i-1).getQuerytweettime();//for merging
						String endID = query.getQuerytweettime();
						outputRoot = origOutputRoot + "/" + query.getNumber()+"/";
						//============================removing spam=================================
						if (spam) System.out.println("**removing spam ...");
							cleanTweetsPath = originalEnglishOnlyTweetsPath;
							startTime = System.currentTimeMillis();
							DetectSpamUsers.detectSpamUsers(args[2], outputRoot+SPAM_USERS_DIRECTORY, Long.parseLong(startID), Long.parseLong(endID), spamUrlThreshold, spamUserThreshold, strictspamURLThreshold); 
							numOfNonSpamTweets = RemoveTweetsOfSpamUsers.removeTweetsOfSpamUsers(originalEnglishOnlyTweetsPath, outputRoot+NON_SPAM_DIRECTORY ,numReducers, 
									outputRoot+SPAM_USERS_DIRECTORY+"//part-r-00000", Long.parseLong(startID), Long.parseLong(endID), outputRoot+NUM_OF_TWEETS_FILE, spam);
							
							endTime = System.currentTimeMillis();	
						if (spam)
						{
							sLogger.info("Removing spam tweets took " + (endTime - startTime) + " milliseconds");
							System.out.println("done.");
							cleanTweetsPath=outputRoot+NON_SPAM_DIRECTORY;
						}else
						{
							System.out.println("**skip removing spam ...");
							cleanTweetsPath=originalEnglishOnlyTweetsPath;
						}
						
						//====================================URL and HashTag Representation==================================================
						System.out.println("url expand?"+urlExpand);
						
							System.out.println("**expanding url ...");
							startTime = System.currentTimeMillis();
							GetUrlRepresentation.getURLRepresentation(cleanTweetsPath, outputRoot+URL_REPRESENTATION_DIRECTORY, startID, endID, urlRepTermFrequencyThreshold, urlRepTopNTerms);
							endTime = System.currentTimeMillis();
							sLogger.info("URL Representation Operation took " + (endTime - startTime) + " milliseconds \n");
							System.out.println("done.");
						
						System.out.println("hashtag expand?"+hashtagExpand);
						
							System.out.println("**expanding hashtag ...");
							startTime = System.currentTimeMillis();
							GetHashtagRepresentation.getHashtagRepresentation(cleanTweetsPath, outputRoot+HASHTAG_REPRESENTATION_DIRECTORY, startID, endID, hashtagRepTermFrequencyThreshold, hashtagRepTopNTerms);
							endTime = System.currentTimeMillis();
							sLogger.info("Hashtag Representation Operation took " + (endTime - startTime) + " milliseconds \n");
							System.out.println("done.");
						
						//====================================Term to User==================================================================
						System.out.println("**building user index ...");
						startTime = System.currentTimeMillis();
						numOfUsers = GetUserTermRepresentation.getUserTermRepresentation(cleanTweetsPath, outputRoot+USER_TERM_REPRESENTATION_DIRECTORY, startID, endID, 
								userRepTermFrequencyThreshold, userRepTopNTerms, outputRoot+NUM_OF_USERS_FILE);
						endTime = System.currentTimeMillis();
						sLogger.info("usernameToTerm Operation took " + (endTime - startTime) + " milliseconds \n");
						startTime = System.currentTimeMillis();
						IndexUsers.indexUsers(outputRoot+USER_TERM_REPRESENTATION_DIRECTORY, outputRoot+USERS_INDEX_DIRECTORY);
						endTime = System.currentTimeMillis();
						sLogger.info("termToUsername Operation took " + (endTime - startTime) + " milliseconds \n");
						System.out.println("done.");
						//====================================Build Index and Its Forward Index=============================================
						System.out.println("building index ...");
						startTime = System.currentTimeMillis();
						String outputIndexName = "";
						if (urlExpand) outputIndexName+="+urlExp";
						if (hashtagExpand) outputIndexName+="+hashtagExp";
						IndexTweets.indexTweets(cleanTweetsPath, outputRoot+outputIndexName+TWEETS_INDEX_DIRECTORY, numReducers, 
								outputRoot+HASHTAG_REPRESENTATION_DIRECTORY+"//part-r-00000", outputRoot+URL_REPRESENTATION_DIRECTORY+"//part-r-00000", 
								startID, endID, hashtagExpand, urlExpand);
						System.out.println("done.");
						System.out.println("building fwd index ...");
						BuildPostingsForwardIndex.RunBuildPostingForwardIndex(outputRoot+outputIndexName+TWEETS_INDEX_DIRECTORY,
								outputRoot+outputIndexName+TWEETS_INDEX_FORWARD_DIRECTORY);
						endTime = System.currentTimeMillis();
						sLogger.info("Index and Posting Building took " + (endTime - startTime) + " milliseconds \n");
						System.out.println("done.");
						//====================================Retrieval===============================================
						System.out.println("query eval ...");
						startTime = System.currentTimeMillis();
						PostingsForwardIndex findex = new PostingsForwardIndex(outputRoot+outputIndexName+TWEETS_INDEX_DIRECTORY, outputRoot+outputIndexName+TWEETS_INDEX_FORWARD_DIRECTORY, FileSystem.get(conf));
						UserIndex userIndex=new UserIndex(new Path(outputRoot+USERS_INDEX_DIRECTORY+"//part-r-00000"), FileSystem.get(conf));
						//numOfNonSpamTweets = FSProperty.readInt(FileSystem.get(conf), outputRoot+NUM_OF_TWEETS_FILE);
						//numOfUsers = FSProperty.readInt(FileSystem.get(conf), outputRoot+NUM_OF_USERS_FILE);
						TreeSet<TweetScore> rl = QueryEval.runQuery(dbConnection, Long.parseLong(query.getQuerytweettime()), numOfNonSpamTweets, numOfUsers, 
								findex, userIndex, query.getTitle(), topNTweetsPerQuery, primaryRankingFn, secondaryRankingFn);
						endTime = System.currentTimeMillis();
						sLogger.info("Retrieval took " + (endTime - startTime) + " milliseconds \n");
						QueryEval.writeQueryResults(runID, query.getNumber(), rl, resultsOut);
						endQuery = System.currentTimeMillis();
						sLogger.info("> The entire query took " + (endQuery - startQuery) + " milliseconds \n");
						System.out.println("done.");
						i++;
						System.out.println("End of query("+query.getNumber()+") ------------------------------");
					}//end of for
					resultsOut.close();
					endRun = System.currentTimeMillis();
					sLogger.info(">> The entire run took " + (endRun - startRun) + " milliseconds \n");
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
