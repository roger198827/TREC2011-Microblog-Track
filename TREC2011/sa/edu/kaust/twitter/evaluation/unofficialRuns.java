package sa.edu.kaust.twitter.evaluation;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;



public class unofficialRuns {
	
	public static void main(String[] args)
	{
		Boolean all=false;
		int relevantNumber=0, numberOfConsideredQuery=1;
		int total=0;
		File file = new File("microblog11-qrels.txt");
	    FileInputStream fis = null;
	    BufferedInputStream bis = null;
	    DataInputStream dis = null;
	    String ALine;
	    String tweetID="";
	    ArrayList<String>relevant=new ArrayList<String>();
	    //FileWriter fstream;
	    try {
	        fis = new FileInputStream(file);
	        // Here BufferedInputStream is added for fast reading.
	        bis = new BufferedInputStream(fis);
	        dis = new DataInputStream(bis);
	        String queryNumber="", lastQueryNumber="1", queryNumberToAppend="";
	        while (dis.available() != 0) {	        	
	           ALine=dis.readLine();
	           //out.write(ALine+"\n");
	           StringTokenizer tokens = new StringTokenizer(ALine);	          
	           for (int y=0;y<=3;y++) 
	        	   {
	        	   		if (y==0)
	        	   		{
	        	   			queryNumber=tokens.nextToken();
	        	   			if (Integer.parseInt(queryNumber)<=9)queryNumber="0"+queryNumber;

	        	   		}
	        	   		else if (y==2) 
	        	   		{
	        	        	tweetID=queryNumber+tokens.nextToken();//append queryNumber before tweetID
	        	        	//System.out.println(tweetID);
	        	   		}
	        	        else if (y==3) {	        	   			
	        	   			int indicator=Integer.parseInt(tokens.nextToken());
	        	   			if(all)
	        	   			{
		        	   			if(indicator==1||indicator==2){
			        	   			  numberOfConsideredQuery=49;
			        	   			  relevant.add(tweetID);
		        	   			  }
	        	   			}else	 
	        	   			{
	        	   				if(indicator==2)
		        	   			{
			        	   			numberOfConsideredQuery=33;
			        	   			relevant.add(tweetID);
		        	   			}
	        	   			}
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
	        int rank=0;
	        String queryID="", lastQueryID="MB001";
	        while (dis.available() != 0){
	        	ALine=dis.readLine();
	        	StringTokenizer tokens = new StringTokenizer(ALine);
	        	for (int y=0;y<=3;y++) 
	        	   {
	        			if (y==0) //save queryID
	        			{
	        				queryID=tokens.nextToken();
	        				if(!queryID.equalsIgnoreCase(lastQueryID))
	        				{
	        					System.out.println(lastQueryID+ " retrieve "+rank+" tweets");
	        				}
	        				lastQueryID=queryID;
	        			}else if (y==2) //save tweetID
	        	        {
	        	        	tweetID=queryID.substring(3, 5)+tokens.nextToken();//append query number to tweetID
	        	        	//System.out.println(tweetID);
	        	        	rank=Integer.parseInt(tokens.nextToken());
	        	        	if (rank<=30)
	        	        	{
	        	        		if(relevant.contains(tweetID))
	        	        		relevantNumber++;
	        	        		//total++;
	        	        	}
	        	        }
	        	        else{
	        	        	tweetID="";
	        	        	tokens.nextToken();
	        	        }
	        	   }
	        }
	        System.out.println("Number of Relevant Retrieved= "+relevantNumber);
	        System.out.println("Number of Actual Relevant= "+relevant.size());
	        //System.out.println(total);
	        int denominator= 30* numberOfConsideredQuery;
	        System.out.println("Number of Considered Query= "+numberOfConsideredQuery);
	        System.out.println("Precision @30= "+(float)relevantNumber/denominator);
	        
	}catch (Exception e) {
	    e.printStackTrace();
	  }
	}
}
