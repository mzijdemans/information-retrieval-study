package assignment2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class QueryCode {

	  public static List<Query> queries = setQueries();

	  public static List<Query> setQueries() {

		  List<Query> queries = new ArrayList<Query>();
		  try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder parser = factory.newDocumentBuilder();
			Document doc = parser.parse("training-queries-no-filter.txt"); 
		    NodeList nodes = doc.getElementsByTagName("DOC");
		    for(int i=0; i< nodes.getLength(); i++) {
		    	Element e = (Element) nodes.item(i);
		    	String docNo = e.getElementsByTagName("DOCNO").item(0).getTextContent();    
		        String text = e.getElementsByTagName("TEXT").item(0).getTextContent();
		        List<String> words = new ArrayList<String>();
		        StringTokenizer str = new StringTokenizer(text);
		        while(str.hasMoreTokens()){
		      		 words.add(str.nextToken().trim());
		        }
		        queries.add(new Query(words, docNo));
		    }
		    return queries;
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}
	}
	  
	public static void main(String[] args) {
		for(Query q : queries) {
			String s = "list = Arrays.asList(\"";
			for(String t : q.terms) {
				s += t+"\",\"";
			}
			s = s.substring(0, s.length()-2);
			s += ");";
			System.out.println(s);
			System.out.println("queriesList.add(new Query(list,\""+q.queryNo+"\"));");
		}
		
		
	}

}
