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

public class QueryProcessing {

	
	public static HashMap<String, String> stopwords = setStopWords();
	  public static HashMap<String, String> setStopWords(){
	  	HashMap<String, String> stopwords = new HashMap<String, String>(41);  
	  	  	  stopwords.put("an","");
			  stopwords.put("he","");
			  stopwords.put("she","");
			  stopwords.put("us","");
			  stopwords.put("this","");
			  stopwords.put("their","");
			  stopwords.put("be","");
			  stopwords.put("with","");
			  stopwords.put("from","");
			  stopwords.put("or","");
			  stopwords.put("as","");
			  stopwords.put("by","");
			  stopwords.put("was","");
			  stopwords.put("that","");
			  stopwords.put("for","");
			  stopwords.put("are","");
			  stopwords.put("on","");
			  stopwords.put("it","");
			  stopwords.put("is","");
			  stopwords.put("to","");
			  stopwords.put("a","");
			  stopwords.put("in","");
			  stopwords.put("and","");
			  stopwords.put("of","");
			  stopwords.put("the","");
			  stopwords.put("thei","");
			  stopwords.put("they","");
			  stopwords.put("i","");
			  stopwords.put("hi","");
			  stopwords.put("at","");
			  stopwords.put("us","");
			  stopwords.put("which","");
			  stopwords.put("had","");
			  stopwords.put("have","");
			  stopwords.put("do","");
			  stopwords.put("it","");
			  stopwords.put("us","");
			  stopwords.put("hi","");
			  stopwords.put("you","");
			  stopwords.put("but","");
			  stopwords.put("who","");
			  stopwords.put("on","");
			  return stopwords;
	    }
	  public static List<Query> queries = setQueries();

	  public static List<Query> setQueries() {

		  List<Query> queries = new ArrayList<Query>();
		  try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder parser = factory.newDocumentBuilder();
			Document doc = parser.parse("training-queries.txt"); 
		    NodeList nodes = doc.getElementsByTagName("DOC");
		    for(int i=0; i< nodes.getLength(); i++) {
		    	Element e = (Element) nodes.item(i);
		    	String docNo = e.getElementsByTagName("DOCNO").item(0).getTextContent();    
		        String text = e.getElementsByTagName("TEXT").item(0).getTextContent();
		        List<String> words = new ArrayList<String>();
		        StringTokenizer str = new StringTokenizer(text);
		        String word = "";
		        while(str.hasMoreTokens()){
		      	  word = str.nextToken().toLowerCase();
		      	  boolean skip = false;
		      	  word = word.replaceAll("[^\\p{Alpha}]+","");
		      	  if(word.matches("[a-z]+")) //[A-Z]* incase casefolding is turned off...
			       	  skip = false;
		      	  else
			       	  skip = true;
		    	  if(!skip)
		    		  skip = stopwords.containsKey(word);
		      	  if(!skip){
		      		  Stemmer stemmer = new Stemmer();
		      		  stemmer.add(word.toCharArray(), word.length());
		      		  stemmer.stem();
		      		  word = stemmer.toString();
		      	  }
		      	  if(!skip){
		      		 words.add(word);
		      	  }
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
