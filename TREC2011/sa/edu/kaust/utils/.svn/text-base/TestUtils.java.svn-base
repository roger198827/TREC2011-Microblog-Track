package sa.edu.kaust.utils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;

import org.apache.hadoop.fs.Path;

import sa.edu.kaust.twitter.data.Tweet;
import sa.edu.kaust.twitter.data.TweetCollectionReader;
import sa.edu.kaust.twitter.dbconnection.Db;
import sa.edu.kaust.twitter.dbconnection.MySqlConnection;

public class TestUtils {
	
	//read All files
	
	//temporary test
	public static void main(String[] args) {
		
		/*test convert time stamp*/
		String date= "Mon Jan 24 00:00:14 +0000 2011";
		String formatted = Util.convertTimeStamp(date);
		System.out.println(formatted);
		
		/*testgetEndOfUserName*/
		String tweet= "tweet: ??????????????????????????????????????????????????(@_@)???????????????????????????";
		System.out.println("name:= "+tweet.substring(8,Util.getEndOfUserName(tweet.substring(8))+8));
		
	}

}
