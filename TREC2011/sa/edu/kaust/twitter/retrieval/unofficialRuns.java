package sa.edu.kaust.twitter.retrieval;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;



public class unofficialRuns {
	
	public static void main(String[] args)
	{
		//1 : path of tweets collection
		//2 : file path of forward index
		//3 : qrel path
		int relevantNumber=0;
		int total=0;
		File file = new File("microblog11-qrels.txt");
	    FileInputStream fis = null;
	    BufferedInputStream bis = null;
	    DataInputStream dis = null;
	    String ALine, tweetID="";
	    ArrayList<Long>relevant=new ArrayList<Long>();
	    //FileWriter fstream;
	    try {
	        fis = new FileInputStream(file);
	        // Here BufferedInputStream is added for fast reading.
	        bis = new BufferedInputStream(fis);
	        dis = new DataInputStream(bis);
	        
	        while (dis.available() != 0) {	        	
	           ALine=dis.readLine();
	           //out.write(ALine+"\n");
	           StringTokenizer tokens = new StringTokenizer(ALine);	          
	           for (int y=0;y<=3;y++) 
	        	   {
	        	        if (y==2) 
	        	        	tweetID=tokens.nextToken();
	        	        else if (y==3) {	        	   			
	        	   			int indicator=Integer.parseInt(tokens.nextToken());
	        	   			//if(indicator==1||indicator==2)
	        	   			if(indicator==2)
	        	   			relevant.add(Long.parseLong(tweetID));
	        	   		}
	        	   		else
	        	   			{
	        	   				tweetID="";
	        	   				tokens.nextToken();
	        	   			}
	        	   }	          
	        }
	       	        
	        fis.close();
	        bis.close();
	        dis.close();
	        
	        fis = new FileInputStream(new File("KAUSTRerank.txt"));
	        // Here BufferedInputStream is added for fast reading.
	        bis = new BufferedInputStream(fis);
	        dis = new DataInputStream(bis);
	        while (dis.available() != 0){
	        	ALine=dis.readLine();
	        	StringTokenizer tokens = new StringTokenizer(ALine);
	        	for (int y=0;y<=2;y++) 
	        	   {
	        	        if (y==2) 
	        	        {
	        	        	tweetID=tokens.nextToken();
	        	        	if(relevant.contains(Long.parseLong(tweetID)))
	        	        		relevantNumber++;
	        	        	total++;
	        	        }
	        	        else{
	        	        	tweetID="";
	        	        	tokens.nextToken();
	        	        }
	        	   }
	        }
	        System.out.println(relevant.size());
	        System.out.println(relevantNumber);
	        System.out.println(total);
	        System.out.println((float)relevantNumber/total);
	        
	}catch (Exception e) {
	    e.printStackTrace();
	  }
	}
}
