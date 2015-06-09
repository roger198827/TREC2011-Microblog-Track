package sa.edu.kaust.twitter.retrieval;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class QueryParser {
	
	public static void prepareFile(String filePath)
	{
		File file = new File(filePath);
	    FileInputStream fis = null;
	    BufferedInputStream bis = null;
	    DataInputStream dis = null;
	    String ALine;
	    FileWriter fstream;
	    try {
	        fis = new FileInputStream(file);
	        // Here BufferedInputStream is added for fast reading.
	        bis = new BufferedInputStream(fis);
	        dis = new DataInputStream(bis);
	        fstream = new FileWriter(filePath+".xml");
	        BufferedWriter out = new BufferedWriter(fstream);
	        out.write("<root>\n");
	        while (dis.available() != 0) {	        	
	           ALine=dis.readLine();
	           out.write(ALine+"\n");
	        }
	        out.write("</root>");
	        out.close();
	        fis.close();
	        bis.close();
	        dis.close();
	    }
	    catch (FileNotFoundException e) {
	      e.printStackTrace();
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	}
	
	//public static PriorityQueue<Query> ReadQuery(String filePath) {
	      @SuppressWarnings("unchecked")
		public static ArrayList<Query> readQueries(String filePath) {
		  //ValueComparator pvc= new ValueComparator();
		  //PriorityQueue queryList = new PriorityQueue(50,pvc);
	      ArrayList<Query> queryList = new ArrayList<Query>();
		  try {
		  File file = new File(filePath);
		  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		  DocumentBuilder db = dbf.newDocumentBuilder();
		  Document doc = db.parse(file);
		  doc.getDocumentElement().normalize();
		  NodeList nodeLst = doc.getElementsByTagName("top");
		  //System.out.println("Information of all topics");
		  for (int s = 0; s < nodeLst.getLength(); s++) {

		  Node fstNode = nodeLst.item(s);
		  Query newQuery = new Query();
		    
		  if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
		  
		      Element fstElmnt = (Element) fstNode;
		      
		      NodeList numElmntLst = fstElmnt.getElementsByTagName("num");
		      Element numElmnt = (Element) numElmntLst.item(0);
		      NodeList num = numElmnt.getChildNodes();
		      //System.out.println("Number : "  + ((Node) fstNm.item(0)).getNodeValue().split(" ")[2]);
		      newQuery.setNumber(((Node) num.item(0)).getNodeValue().split(" ")[2]);
		      
		      NodeList titleElmntLst = fstElmnt.getElementsByTagName("title");
		      Element titleElmnt = (Element) titleElmntLst.item(0);
		      NodeList title = titleElmnt.getChildNodes();
		      //System.out.println("Title : " + ((Node) lstNm.item(0)).getNodeValue());
		      newQuery.setTitle(((Node) title.item(0)).getNodeValue());
		      
		      NodeList queryTimeElmntLst = fstElmnt.getElementsByTagName("querytime");
		      Element queryTimeElmnt = (Element) queryTimeElmntLst.item(0);
		      NodeList queryTime= queryTimeElmnt.getChildNodes();
		      //System.out.println("querytime : " + ((Node) queryTime.item(0)).getNodeValue());
		      newQuery.setQuerytime(((Node) queryTime.item(0)).getNodeValue());
		      
		      NodeList queryTweetTimeElmntLst = fstElmnt.getElementsByTagName("querytweettime");
		      Element queryTweetTimeElmnt = (Element) queryTweetTimeElmntLst.item(0);
		      NodeList queryTweetTime= queryTweetTimeElmnt.getChildNodes();
		      //System.out.println("querytweettime : " + ((Node) queryTweetTime.item(0)).getNodeValue());
		      newQuery.setQuerytweettime(((Node) queryTweetTime.item(0)).getNodeValue().trim());
		      queryList.add(newQuery);
		    }//end of if
		  }//end of for
		  //Collections.sort(queryList);
		  } catch (Exception e) {
		    e.printStackTrace();
		  }
		  return queryList;
		}//end of fucntion
	
		@SuppressWarnings("unchecked")
		public static void main(String[] args)
		{
			prepareFile("query.xml");
			ArrayList<Query> mine= readQueries("query.xml.xml");
			Collections.sort(mine);
			for (Query q: mine)
			{
				System.out.println(q.getQuerytweettime());
			}

		}
}

