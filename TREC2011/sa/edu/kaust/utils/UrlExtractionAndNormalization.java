package sa.edu.kaust.utils;



import java.io.*;
import java.util.LinkedList;
import java.util.Random;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umd.cloud9.io.pair.PairOfStringInt;

import sa.edu.kaust.twitter.data.Tweet;
import sa.edu.kaust.utils.*;

public class UrlExtractionAndNormalization {  


	public static PairOfStringInt extractAndNormalizeURL(String message, Boolean getOriginal) throws IOException{    

		Pattern p = Pattern.compile("\\(?\\b(http://|www[.])[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]",Pattern.CASE_INSENSITIVE );    
		Matcher m=p.matcher(message);
		String urlStr= null;
		int start = -1;
		PairOfStringInt url = new PairOfStringInt();

		if(!m.find()) return null;

		urlStr = m.group();
		start = m.start();
		if (urlStr.startsWith("(") && urlStr.endsWith(")"))	    
			urlStr = urlStr.substring(1, urlStr.length() - 1);

		if(!getOriginal){
			url.set(urlStr, start);
			return url;            // return shorted url
		}

		URLConnection urlConn =  connectURL(urlStr);
		try{
			urlConn.getHeaderFields();
		}
		catch(Exception e){
			System.out.println("error");
			return null;
		}
		//System.out.println("Original URL is: "+urlConn.getURL().toString());
		url.set(urlConn.getURL().toString(), start);
		return url;
	}

	public static PairOfStringInt extractURL(String message){    
		Pattern p = Pattern.compile("\\(?\\b(http://|www[.])[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]",Pattern.CASE_INSENSITIVE );    
		Matcher m=p.matcher(message);
		String urlStr= null;
		int start = -1;
		PairOfStringInt url = new PairOfStringInt();

		if(!m.find()) return null;

		urlStr = m.group();
		start = m.start();
		if (urlStr.startsWith("(") && urlStr.endsWith(")")){
			start++;
			urlStr = urlStr.substring(1, urlStr.length() - 1);
		}
		url.set(urlStr, start);
		return url;            // return shorted url
	}

	

	//connectURL - This function will take a valid url and return a URL object representing the url address.
	public static URLConnection connectURL(String strURL) {
		URLConnection conn =null;
		try {
			URL inputURL = new URL(strURL);
			conn = inputURL.openConnection();

		}catch(MalformedURLException e) {
			System.out.println("Please input a valid URL");
		}catch(IOException ioe) {
			System.out.println("Can not connect to the URL");
		}
		return conn;

	}


	
}