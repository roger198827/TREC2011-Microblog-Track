package sa.edu.kaust.twitter.tokenize;

public class TweetToken {
	
	public static enum TweetTokenType {
		MENTION("mention"),
		RT("rt"),
		HASHTAG("hashtag"),
		URL("url"),
		OTHER("other");

		public final String name;

		TweetTokenType(String s) {
			name = s;
		}
	};

	public String text;
	public TweetTokenType type;
	public int startPos;
	
	public TweetToken(int startPos, String text, TweetTokenType type){
		this.startPos = startPos;
		this.text = text;
		this.type = type;
	}

	@Override
	public String toString() {
		return "["+type.name.charAt(0)+", "+startPos+":"+text+"]";
	}
}
