package sa.edu.kaust.twitter.retrieval;

public class Query implements Comparable{
	
	private String number;
	private String title;
	private String querytime;
	private String querytweettime;
	
	void setNumber(String number) {
		this.number = number;
	}
	public String getNumber() {
		return number;
	}
	
	void setTitle(String title) {
		this.title = title;
	}
	public String getTitle() {
		return title;
	}
	
	void setQuerytime(String querytime) {
		this.querytime = querytime;
	}
	public String getQuerytime() {
		return querytime;
	}
	
	void setQuerytweettime(String querytweettime) {
		this.querytweettime = querytweettime;
	}
	public String getQuerytweettime() {
		return querytweettime;
	}
	@Override
	public String toString() {
		return "Query [q#: "+number+", title: "+title+ ", qtime: "+querytime+", qtweettime: "+querytweettime+"]";
	}
	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		if(Long.parseLong(this.getQuerytweettime())>Long.parseLong(((Query)arg0).getQuerytweettime())) {// to get the biggest value on top of the tree
	    	//System.out.println("left is bigger");
	      return 1;
	    } else if(Long.parseLong(this.getQuerytweettime())==Long.parseLong(((Query)arg0).getQuerytweettime())) {
	      return 0;
	    } else {
	      return -1;
	    }
	}

}
