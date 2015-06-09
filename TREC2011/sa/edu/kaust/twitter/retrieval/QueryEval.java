package sa.edu.kaust.twitter.retrieval;

import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import sa.edu.kaust.twitter.dbconnection.NewDb;
import sa.edu.kaust.twitter.index.BuildTweetsForwardIndex;
import sa.edu.kaust.twitter.index.PostingsForwardIndex;
import sa.edu.kaust.twitter.index.TweetsForwardIndex;
import sa.edu.kaust.twitter.index.data.PostingsReader;
import sa.edu.kaust.twitter.index.data.TweetPosting;
import sa.edu.kaust.twitter.index.data.TweetPostingsList;
import sa.edu.kaust.twitter.preprocess.user.UserIndex;
import sa.edu.kaust.twitter.retrieval.score.GlobalEvidence;
import sa.edu.kaust.twitter.retrieval.score.GlobalTermEvidence;
import sa.edu.kaust.twitter.retrieval.score.IDFScoringFunction;
import sa.edu.kaust.twitter.retrieval.score.ScoringFunction;
import sa.edu.kaust.twitter.retrieval.score.TweetScoringFunction;
import sa.edu.kaust.twitter.tokenize.TweetToken;
import sa.edu.kaust.twitter.tokenize.TweetTokenizer;

import edu.umd.cloud9.io.FSProperty;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.util.map.HMapKF;

public class QueryEval {
	private static final Logger sLogger = Logger.getLogger(QueryEval.class);
	{
		sLogger.setLevel (Level.ERROR);
	}

	//static ScoringFunction contentSimScoreFn = new TFIDFScoringFunction();
	static ScoringFunction contentSimScoreFn = new IDFScoringFunction();
	//static ScoringFunction userAuthorityScoreFn = new TFIDFScoringFunction();
	static ScoringFunction userAuthorityScoreFn = new IDFScoringFunction();
	/*static float contentSimilarityWt = 0.5f;
	static float userAuthorityWt = 0.5f;
	static float recencyWt = 0.5f;
	static float tweetPopularityWt = 0.5f;
	static float userPopularityWt = 0.5f;
	static float urlPopularityWt = 0.5f;*/

	public static TreeSet<TweetScore> queryEval(NewDb db, long queryTweetID, ArrayList<TweetPostingsList> list,
			GlobalEvidence ge, HMapKF<String> userAuthority, int topN, TweetScoringFunction primaryRankingFn) throws IllegalStateException, SQLException{
		//sLogger.setLevel(Level.INFO);
		sLogger.info("===QE");
		int nLists = list.size();
		sLogger.info(">> Intersecting a list of "+nLists +" terms");

		// a reader for each pl
		PostingsReader[] reader = new PostingsReader[nLists];

		// the cur posting of each list
		TweetPosting[] posting = new TweetPosting[nLists];

		// min-heap for union
		java.util.PriorityQueue<DocList> minHeap = new java.util.PriorityQueue<DocList>(nLists, comparator);

		int i = 0;
		for(TweetPostingsList pl : list){
			//sLogger.info("\tL:"+i+" "+pl.getDf()+" "+pl.getCf());
			reader[i] = pl.getPostingsReader();

			posting[i] = new TweetPosting();
			reader[i].nextPosting(posting[i]);
			minHeap.add(new DocList(posting[i].getTweetID(), i));
			i++;
		}
		DocList dl;
		long curDoc;
		TweetPostingsList curPL;
		TreeSet<TweetScore> rankedList = new TreeSet<TweetScore>();
		float docScore;
		GlobalTermEvidence gte = new GlobalTermEvidence();
		int curDocLength;
		//float contentSimScore, recencyScore, userPopularityScore, tweetPopularityScore, urlPopularityScore, userTopicAuthorityScore;
		int nTweetsSoFar = 0;
		while (minHeap.size() > 0 && nTweetsSoFar < 100000) {
			//sLogger.info("Heap size: "+heap.size()+" peek = "+heap.peek());

			// get the minimum docno that has a query term
			dl = minHeap.remove();
			i = dl.listIndex;
			curDoc = dl.id;
			//curDocLength = dlTable.getDocLength(curDoc);
			curDocLength = 10;
			//sLogger.info("D:"+curDoc+" ("+curDocLength+")");
			curPL = list.get(i);
			// compute score for that docno for that term
			gte.set(curPL.getDf(), curPL.getCf());
			contentSimScoreFn.initialize(gte, ge);
			docScore=(float)contentSimScoreFn.getScore(posting[i].getTF(), curDocLength);
			//sLogger.info("\t\tL:"+i+" S:"+docScore);
			// add next docno from same postings list
			if(reader[i].nextPosting(posting[i])){
				//reader[i].getPositions(tp[i]);
				dl.set(posting[i].getTweetID(), i);
				minHeap.add(dl);
			}
			// check if this docno has other query terms as well
			while(minHeap.size() > 0){
				//DocList nextDL = heap.peek();
				dl = minHeap.peek();
				if(dl.id != curDoc) break;
				dl = minHeap.remove();
				i = dl.listIndex;
				curPL = list.get(i);
				// add score for that docno for that term
				gte.set(curPL.getDf(), curPL.getCf());
				contentSimScoreFn.initialize(gte, ge);
				docScore+=(float)contentSimScoreFn.getScore(posting[i].getTF(), curDocLength);	//getScore(tf,doclen)
				//sLogger.info("\t\tL:"+i+" S:"+docScore);
				if(reader[i].nextPosting(posting[i])){
					//reader[i].getPositions(tp[i]);
					dl.set(posting[i].getTweetID(), i);
					minHeap.add(dl);
				}		
			}
			TweetScore ts = new TweetScore();
			ts.tweetID = -curDoc;
			//---------------- other features
			ts.contentSimScore = docScore;
			//docScore *= contentSimilarityWt;
			/*String username = db.getUserName(ts.tweetID+"");
			if(primaryRankingFn.userPopularityWt > 0.01) ts.userPopularityScore = db.getUserPopularity(username, 0.25f, 0.5f, 0.25f, queryTweetID)/10.0f;
			if(primaryRankingFn.userAuthorityWt > 0.01) ts.userTopicAuthorityScore = userAuthority.get(username)/5.0f;
			if(primaryRankingFn.tweetPopularityWt > 0.01) ts.tweetPopularityScore = db.getCountBeingRetweeted(ts.tweetID, queryTweetID)/5.0f;
			if(primaryRankingFn.urlPopularityWt > 0.01) ts.urlPopularityScore = db.getTweetURLCount(ts.tweetID, queryTweetID)/3.0f;*/
			//docScore += userPopularityWt * userPopularity;
			//docScore += tweetPopularityWt * retweetNumber;
			//docScore += urlPopularityWt * urlFreq;
			//docScore += userAuthorityWt * userTopicAuthority;
			//ts.recencyScore= ts.tweetID/(float)queryTweetID;
			//----------------
			//docScore =  primaryRankingFn.getScore(contentSimScore, recencyScore, userPopularityScore, tweetPopularityScore, urlPopularityScore, userTopicAuthorityScore);
			docScore =  primaryRankingFn.getScore(ts);
			//sLogger.debug("added "+pair.toString());
			// add document to the ranked list
			ts.score = docScore; 

			rankedList.add(ts);
			nTweetsSoFar++;
			if(rankedList.size()>topN){
				// remove the minimum score
				rankedList.pollFirst();
			}
		}
		minHeap.clear();
		sLogger.info("===Done");
		//sLogger.setLevel(Level.WARN);
		return rankedList;
	}

	public static class DocList{
		public long id;
		public int listIndex;
		/**
		 * @param id
		 * @param listIndex
		 */
		public DocList(long id, int listIndex) {
			this.id = id;
			this.listIndex = listIndex;
		}

		public void set(long id, int listIndex) {
			this.id = id;
			this.listIndex = listIndex;
		}

		@Override
		public String toString() {
			return "{"+id+" - "+listIndex+"}";
		}

	}

	public static class DocListComparator implements Comparator<DocList>{

		public int compare(DocList t1, DocList t2) {
			if(t1.id < t2.id) return -1;
			else if(t1.id > t2.id) return 1;
			return 0;
		}
	}

	private static DocListComparator comparator = new DocListComparator();

	public static class TweetScoreComparatorByID implements Comparator<TweetScore>{

		public int compare(TweetScore t1, TweetScore t2) {
			if(t1.tweetID < t2.tweetID) return -1;
			else if(t1.tweetID > t2.tweetID) return 1;
			return 0;
		}
	}

	private static TweetScoreComparatorByID tweetScoreComparatorByID  = new TweetScoreComparatorByID();

	public static void printRankedList(TreeSet<TweetScore> rl){
		for(TweetScore p: rl){
			System.out.println(p);
		}
	}
	public static TreeSet<TweetScore> runQuery(NewDb db, long queryTweetID, int totalNumOfTweets, int totalNumOfUsers, 
			PostingsForwardIndex postingsFwdIndex, UserIndex userIndex, String query, 
			int topN, TweetScoringFunction primaryRankingFn, TweetScoringFunction secondaryRankingFn) throws Exception {

		ArrayList<TweetToken> terms;

		ArrayList<TweetPostingsList> listOfTermPostingsLists = new ArrayList<TweetPostingsList>();
		int querylen;
		GlobalEvidence ge;
		TweetPostingsList pl;
		listOfTermPostingsLists.clear();
		terms = TweetTokenizer.getTokenStream(query);
		for(TweetToken term : terms){
			pl = postingsFwdIndex.getValue(term.text);
			if(pl != null){
				listOfTermPostingsLists.add(pl);
			}
		}
		querylen = listOfTermPostingsLists.size();
		ge = new GlobalEvidence(totalNumOfTweets, 0, querylen);
		HMapKF<String> userAuthority = getUserTopicAuthority(totalNumOfUsers, userIndex, terms);
		TreeSet<TweetScore> rl = queryEval(db, queryTweetID, listOfTermPostingsLists, ge, userAuthority, /*DocLengthTable dlTable,*/ topN, primaryRankingFn);
		if(secondaryRankingFn != null) {
			rl = rerank(db, userAuthority, queryTweetID, rl, 50, secondaryRankingFn);
			rl = rerankByTimeStamp(rl, 30);
		}
		else rl = rerankByTimeStamp(rl, 30);
		//printRankedList(rl);
		//System.out.println();
		//rl = rerankByTimeStamp(rl, 50);
		//printRankedList(rl);
		// clear
		for(TweetPostingsList plist : listOfTermPostingsLists) plist.clear();
		listOfTermPostingsLists.clear();
		userAuthority.clear();
		return rl;
	}

	static TreeSet<TweetScore> rerank(NewDb db, HMapKF<String> userAuthority, long queryTweetID, TreeSet<TweetScore> rl, int topN, TweetScoringFunction secondaryRankingFn) throws IllegalStateException, SQLException{
		System.out.println("reranking ...");
		TreeSet<TweetScore> newRL = new TreeSet<TweetScore>();
		float newScore = 0;
		for(TweetScore ts: rl){
			TweetScore newTS = new TweetScore();
			newTS.tweetID = ts.tweetID;
			newTS.contentSimScore = ts.contentSimScore;

			String username = db.getUserName(ts.tweetID+"");
			if(secondaryRankingFn.userPopularityWt > 0) newTS.userPopularityScore = db.getUserPopularity(username, 0.25f, 0.5f, 0.25f, queryTweetID)/10.0f;
			if(secondaryRankingFn.userAuthorityWt > 0) newTS.userTopicAuthorityScore = userAuthority.get(username)/5.0f;
			if(secondaryRankingFn.tweetPopularityWt > 0) newTS.tweetPopularityScore = db.getCountBeingRetweeted(ts.tweetID, queryTweetID)/5.0f;
			if(secondaryRankingFn.urlPopularityWt > 0) newTS.urlPopularityScore = db.getTweetURLCount(ts.tweetID, queryTweetID)/3.0f;

			newScore =secondaryRankingFn.getScore(newTS);
			newTS.score = newScore;
			newRL.add(newTS);
			if(newRL.size()>topN){
				// remove the minimum score
				newRL.pollFirst();
			}
		}
		System.out.println("done.");
		return newRL;
	}

	static TreeSet<TweetScore> rerankByTimeStamp(TreeSet<TweetScore> rl, int topN){
		TreeSet<TweetScore> newRL = new TreeSet<TweetScore>(tweetScoreComparatorByID);
		for(TweetScore ts: rl){
			newRL.add(ts);
			if(newRL.size()>topN){
				// remove the minimum score
				newRL.pollFirst();
			}
		}
		return newRL;
	}

	public static void writeQueryResults(String runID, String queryID, TreeSet<TweetScore> rl, BufferedWriter out) throws IOException{
		//BufferedWriter out = new BufferedWriter(new FileWriter(fname));
		System.out.println("Writing results of "+queryID+" ("+rl.size()+" entries) ...");
		int i = 0;
		for(TweetScore ts: rl.descendingSet()){
			i++;
			out.write(queryID + " Q0 " + ts.tweetID + " " + i + " " + ts.score + " " + runID + "\n");
		}
		System.out.println("done.");
	}

	static void writeQueryResultsDebug(String runID, String queryID, TreeSet<TweetScore> rl, TweetsForwardIndex tweetsFwdIndex) throws IOException{
		int i = 0;
		for(TweetScore ts: rl.descendingSet()){
			i++;
			//System.out.println(queryID + " Q0 " + ts.tweetID + " " + i + " " + ts.score + " " + runID);
			System.out.println("["+i+"]("+ts.score+")"+tweetsFwdIndex.getValue(ts.tweetID));
			System.out.println("\t["+"cnt:"+ts.contentSimScore+" time:"+ts.recencyScore+" url:"+
					ts.urlPopularityScore+" ret:"+ts.tweetPopularityScore+" usrpop:"+ts.userPopularityScore + " usrtpc:"+ts.userTopicAuthorityScore+"]");

		}
	}

	static void writeQueryResultsDebug2(String runID, String queryID, TreeSet<TweetScore> rl, TweetsForwardIndex tweetsFwdIndex) throws IOException{
		//BufferedWriter out = new BufferedWriter(new FileWriter(fname));
		int i = 0;
		for(TweetScore ts: rl.descendingSet()){
			i++;
			System.out.println(queryID + " Q0 " + ts.tweetID + " " + i + " " + ts.score + " " + runID);
		}
	}

	/*public static void printAuthoratives(ArrayList<PairOfStringFloat> result){
		for(PairOfStringFloat p: result){
			System.out.println(p.toString());
		}
	}*/
	public static HMapKF<String> getUserTopicAuthority(int totalNumOfUsers, UserIndex userIndex, ArrayList<TweetToken> terms) throws IOException{
		ArrayListWritable<PairOfStringInt> termUserList = null;		
		//ArrayList<PairOfStringFloat>result=new ArrayList<PairOfStringFloat>();
		HMapKF<String> map = new HMapKF<String>();
		float score;
		GlobalEvidence ge = new GlobalEvidence(totalNumOfUsers, 0, terms.size());
		GlobalTermEvidence gte = new GlobalTermEvidence();
		int df;
		for(TweetToken term : terms){
			termUserList=userIndex.getValue(term.text);
			if(termUserList!=null){
				df = termUserList.size();
				for(PairOfStringInt p : termUserList)
				{
					gte.set(df, 0);
					userAuthorityScoreFn.initialize(gte, ge);
					score=(float)userAuthorityScoreFn.getScore(p.getRightElement(), 0);
					//userAuthorityScoreFn
					//float score= (termUserList.get(i).getRightElement()-termUserList.get(termUserList.size()-1).getRightElement()+1)/(termUserList.get(0).getRightElement()-termUserList.get(termUserList.size()-1).getRightElement()+1);
					if(map.containsKey(p.getLeftElement()))
						map.put(p.getLeftElement(),map.get(p.getLeftElement())+score);			    		 
					else
						map.put(p.getLeftElement(), score); 

				}
				termUserList.clear();
			}
		}
		return map;
		/*for(String hashKey:hm.keySet())
		{
			result.add(new PairOfStringFloat(hashKey,hm.get(hashKey)));
		}*/

		//printAuthoratives(result);

	}
	

	public static void main(String[] args) throws Exception
	{
		/*
		<top>
		<num> Number: Example001 </num>
		<title> Chavez expropriate property </title>
		<querytime> Thu Feb 03 22:06:42 +0000 2011 </querytime>
		<querytweettime> 33285274087723010 </querytweettime>
		</top>
		 */

		String rootPath = "/user/telsayed/tweets2011/trec-runs";
		String queryNum = "MB017";
		String queryTweetID = "999999999999999999";
		String query = "anchung law national taiwan";
		boolean expand = false;
		boolean rerank = true;

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

				String postingsPath = rootPath+"/"+queryNum+"/"+(expand?"exp-":"not-exp")+DriverNoDB.TWEETS_INDEX_DIRECTORY;
				String fwindexPath = rootPath+"/"+queryNum+"/"+(expand?"exp-":"not-exp")+DriverNoDB.TWEETS_INDEX_FORWARD_DIRECTORY;
				String userIndexPath = rootPath+"/"+queryNum+"/"+DriverNoDB.USERS_INDEX_DIRECTORY+"/part-r-00000";
				int topN = 100;
				if(!rerank) topN = 50;

				Configuration conf = new Configuration();

				int totalNumOfTweets = FSProperty.readInt(FileSystem.get(conf), rootPath+"/"+queryNum+"/"+DriverNoDB.NUM_OF_TWEETS_FILE);
				int totalNumOfUsers = FSProperty.readInt(FileSystem.get(conf), rootPath+"/"+queryNum+"/"+DriverNoDB.NUM_OF_USERS_FILE);

				String dbUserName = "telsayed";
				String dbPassword = "telsayed";
				long qTweetID = Long.parseLong(queryTweetID);

				System.out.println(queryNum + " id: "+queryTweetID + ": "+ query);
				System.out.println("nTweets: "+rootPath+"/"+queryNum+"/"+DriverNoDB.NUM_OF_TWEETS_FILE + "\nnUsers: "+ rootPath+"/"+queryNum+"/"+DriverNoDB.NUM_OF_USERS_FILE);
				System.out.println("nTweets: "+totalNumOfTweets + " nUsers: "+ totalNumOfUsers);

				FileSystem fs=FileSystem.get(conf);
				TweetsForwardIndex tweetsFwdIndex = null;
				String tweetsPath = null;
				String tweetsFwdIndexPath = null;
				tweetsPath = rootPath+"/"+queryNum+"/"+DriverNoDB.NON_SPAM_DIRECTORY;
				tweetsFwdIndexPath= rootPath+"/"+queryNum+"/non-spam-fwd";
				BuildTweetsForwardIndex.buildTweetsForwardIndex(tweetsPath, tweetsFwdIndexPath);
				tweetsFwdIndex = new TweetsForwardIndex(tweetsPath, tweetsFwdIndexPath, FileSystem.get(conf));
				//TweetScoringFunction secondaryRankingFn=null;
				PostingsForwardIndex postingsFwdIndex = new PostingsForwardIndex(postingsPath, fwindexPath, FileSystem.get(conf));

				NewDb db = new NewDb("localhost:3306", dbUserName, dbPassword);

				//UploadTermToUsername TermToUsername=new UploadTermToUsername(new Path("E:\\CS240OS\\hadoop-0.20.2\\TermToUsername\\part-r-00000"),fs);
				UserIndex userIndex=new UserIndex(new Path(userIndexPath),fs);


				TreeSet<TweetScore> rl = runQuery(db, qTweetID, totalNumOfTweets, totalNumOfUsers, 
						postingsFwdIndex, userIndex, query, topN, primaryRankingFn, secondaryRankingFn);
				writeQueryResultsDebug("KAUSTExpRerank", queryNum, rl, tweetsFwdIndex);
				//writeQueryResultsDebug2("KAUSTExpRerank", queryNum, rl, tweetsFwdIndex);
	}
}
