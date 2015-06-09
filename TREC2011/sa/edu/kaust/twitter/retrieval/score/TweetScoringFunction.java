package sa.edu.kaust.twitter.retrieval.score;

import sa.edu.kaust.twitter.retrieval.TweetScore;

public class TweetScoringFunction {
	public float contentSimilarityWt = 0.5f;
	public float userAuthorityWt = 0.5f;
	public float recencyWt = 0.5f;
	public float tweetPopularityWt = 0.5f;
	public float userPopularityWt = 0.5f;
	public float urlPopularityWt = 0.5f;
	
	public TweetScoringFunction(float contentSimilarityWt,
			float userAuthorityWt, float recencyWt, float tweetPopularityWt,
			float userPopularityWt, float urlPopularityWt) {
		this.contentSimilarityWt = contentSimilarityWt;
		this.userAuthorityWt = userAuthorityWt;
		this.recencyWt = recencyWt;
		this.tweetPopularityWt = tweetPopularityWt;
		this.userPopularityWt = userPopularityWt;
		this.urlPopularityWt = urlPopularityWt;
	}

	public float getScore(float contentSimScore, float recencyScore, float userPopularityScore, 
			float tweetPopularityScore, float urlPopularityScore, float userTopicAuthorityScore){
		float tweetScore = 0;
		tweetScore += contentSimilarityWt * contentSimScore;
		tweetScore += userPopularityWt * userPopularityScore;
		tweetScore += tweetPopularityWt * tweetPopularityScore;
		tweetScore += urlPopularityWt * urlPopularityScore;
		tweetScore += userAuthorityWt * userTopicAuthorityScore;
		tweetScore += recencyWt * recencyScore;
		return tweetScore;
	}
	
	public float getScore(TweetScore ts){
		float tweetScore = 0;
		tweetScore += contentSimilarityWt * ts.contentSimScore;
		tweetScore += userPopularityWt * ts.userPopularityScore;
		tweetScore += tweetPopularityWt * ts.tweetPopularityScore;
		tweetScore += urlPopularityWt * ts.urlPopularityScore;
		tweetScore += userAuthorityWt * ts.userTopicAuthorityScore;
		tweetScore += recencyWt * ts.recencyScore;
		return tweetScore;
	}
}
