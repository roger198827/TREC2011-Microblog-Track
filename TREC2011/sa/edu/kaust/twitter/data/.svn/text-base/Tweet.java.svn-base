package sa.edu.kaust.twitter.data;

import com.twitter.corpus.data.Status;

public class Tweet {
	@Override
	public String toString() {
		//String str = "{tweet:["+id+"]["+code+"]["+message+"][From:"+userName+"][at:"+timeStamp+"]}";
		//String str = "{tweet:["+code+"]["+message+"]";
		String str = "{tweet:["+id+"][From:"+userName+"]["+message+"][at:"+timeStamp+"]}";
		return str;
	}

	protected long id;
	protected String userName;
	protected String timeStamp;
	protected String message;
	protected int code;
	
	public Tweet(long iD, String userN, int cd, String timeSt, String msg)
	{
		id=iD;
		userName = userN.toLowerCase();
		timeStamp = timeSt;
		message = msg;
		code = cd;
	}
	
	public void set(long iD, String userN, int cd, String timeSt, String msg)
	{
		id=iD;
		userName = userN.toLowerCase();
		timeStamp = timeSt;
		message = msg;
		code = cd;
	}
	
	public void set(Status status){
		id=status.getId();
		userName = status.getScreenname().toLowerCase();
		timeStamp = status.getCreatedAt();
		message = status.getText();
		code = status.getHttpStatusCode();
	}
	
	public Tweet(){
		
	}
	
	public long getID() {
		return id;
	}
	
	public String getUserName() {
		return userName;
	}
	
	public String getTimeStamp() {
		return timeStamp;
	}
	
	public String getMessage() {
		return message;
	}		
	
	public int getCode(){
		return code;
	}
	
	/*public void getConversation()
	{
		String[] pair = new String[2];
		LinkedList<String[]> tweetsequence = new LinkedList<String[]>();
		
		String username, temp, message;
		int atIndex=0, colonIndex=0, messageEndIndex=0;
		if (Util.isretweet(getMessage()))
		{
			atIndex= getMessage().indexOf("RT @"); //detecting the retweet, set initial atIndex to pass the while condition
			temp = getMessage();
			//get the username and his/her tweet
			while(atIndex>=0)
			{				
				//locating the username				
				colonIndex = temp.substring(atIndex).indexOf(':');
				username= temp.substring(atIndex+4, atIndex+colonIndex-1);				
				
				pair[0]=username;
				
				//locating the tweet
				temp = temp.substring(atIndex+colonIndex);
				messageEndIndex = temp.indexOf("RT")-1;
				if (messageEndIndex>0)
					message = temp.substring(1, messageEndIndex);
				else
					message = temp.substring(1);
				
				System.out.println(username+" : "+message);
				pair[1]=message;
				tweetsequence.add(pair);
				
				atIndex = temp.indexOf("RT @"); //to update atIndex
			}
		}
	}*/
	
	

	/*private void setID(long iD) {
		id = iD;
	}

	private void setUserName(String userN) {
		userName = userN;
	}

	private void setTimeStamp(String time) {
		timeStamp = time;
	}

	private void setMessage(String msg) {
		message = msg;
	}*/
	
	/*boolean isNonEnglish(String message)
	{
		int count=0;
		String lowerCase=message.toLowerCase();
		for(int i=0;i<lowerCase.length();i++)
		{
			if(message.charAt(i)<'a'||message.charAt(i)>'z')
				count++;
			if(count==10)			
				return true;				
		}
		return false;
	}
	
	boolean hasUrl(String message)
	{
		if(message.contains("http://"))
			return true;
		else 
			return false;
	}
	
	ArrayList getUrl(String message)
	{
	    ArrayList links= new ArrayList();
	    String regex = "\\(?\\b(http://|www[.])[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]";
	    Pattern p = Pattern.compile(regex);
	    Matcher m = p.matcher(message);
	    while(m.find()) {
	    String urlStr = m.group();
	    if (urlStr.startsWith("(") && urlStr.endsWith(")"))
	    {
	    urlStr = urlStr.substring(1, urlStr.length() - 1);
	    }
	    links.add(urlStr);
	    }
        return links;
	}*/
	
	
}
