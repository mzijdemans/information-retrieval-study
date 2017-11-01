package assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import javax.xml.parsers.*;
import org.xml.sax.InputSource;
import org.w3c.dom.*;
import java.io.*;

public class IndexerDocs {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private Text word = new Text();
    private static HashMap<String, String> stopwords = setStopWords();
    
    /*
     * all types of filters to choose from,
     * 
     * choose allFilters to use them all, otherwise
     * make a selection
     * 
     */
    private final boolean allFilters 					= true;
    private final boolean caseFoldingFilter 			= true;
    private final boolean stemmingFilter 				= true;
    private final boolean specialCharsFilter 			= true;
    private final boolean removeTranslationLinksFilter	= true;
    private final boolean stopWordFilter				= true;
    
    private boolean blockParsingTranslations = false;
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
      DocumentBuilderFactory dbf =
          DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(value.toString()));

      Document doc = db.parse(is);
      NodeList nodes = doc.getElementsByTagName("page");

      String id = "";
      String text = "";
      //int tokenPosition = 0;
      Element element = (Element) nodes.item(0);
      id = element.getElementsByTagName("id").item(0).getTextContent();    
      text = element.getElementsByTagName("text").item(0).getTextContent();

      
      StringTokenizer str = new StringTokenizer(text);
      while(str.hasMoreTokens()){
    	  String next = str.nextToken();
    	  //String token = next;
    	  Stemmer stemmer = new Stemmer();
    	  boolean skip = false;
    	  
    	  //[[an:Intelichencia artificial]] for example
    	  if(removeTranslationLinksFilter && next.length() > 2 && (this.blockParsingTranslations || (next.contains(":") && next.substring(0, 2).equals("[[")))){
    		  this.blockParsingTranslations = true;
    		  if(next.substring(next.length()-2, next.length()).equals("]]")){ //end of translation link (wikipedia)
    			  this.blockParsingTranslations = false;
    		  }
    	  }else{
	    	  if(allFilters || caseFoldingFilter){
	    		  next = next.toLowerCase(); // case folding
	    	  }
	    	  if(allFilters || specialCharsFilter){
	    		  next = next.replace("\'s", "");
	    		  next = next.replace("\'", "");
		          next = next.replace("\'", "");
		          next = next.replace("[", "");
		          next = next.replace("]", "");
		          next = next.replace(":", "");
		          next = next.replace(";", "");
		          next = next.replace(".", "");
		          next = next.replace(",", "");
		          next = next.replace("!", "");
		          next = next.replace("?", "");
		          next = next.replace("(", "");
		          next = next.replace(")", "");
		          if(next.matches("[A-Z]*[a-z]+")){ //[A-Z]* incase casefolding is turned off...
		        	  skip = false;
		          }else{
		        	  skip = true;
		          }
		      }
	    	  if(!skip && (allFilters || stopWordFilter)){
	    		  if(stopwords.containsKey(next)){
	    			  next = "";
	    			  skip = true;
	    		  }
		      }
        	  if(allFilters || stemmingFilter){
        		  stemmer.add(next.toCharArray(), next.toCharArray().length);
        		  stemmer.stem();
        		  next = stemmer.toString();
        	  }
        	  word.set(next);
        	  if(!skip){
        		  //context.write(word, one);
        		  context.write(word, new IntWritable(Integer.valueOf(id)));
        		  //context.write(new Text(word.toString() + "#" + id), new IntWritable(tokenPosition));
        		  //context.write(new Text("!!!!!Total: "), one);
        		  
        	  }
    	  }
//      }
//    	  else{
//    		  word.set(next);
//	    	  context.write(word, one);
//	    	  context.write(new Text("!!!!!Total: "), one);
//    	  }
    	//  tokenPosition += (token.length() + 1);
      }
      
      }
      catch (Exception e) {
          e.printStackTrace();
      }
    }
    private static HashMap<String, String> setStopWords(){
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
  }
  
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();
    private StringBuffer list = new StringBuffer("");
    private List<Integer> documentIDs = new ArrayList<Integer>();
    private List<Integer> docs = new ArrayList<Integer>();
    private Map<Integer,Integer> inDocumentFreq = new HashMap<Integer, Integer>();
    private Integer one = new Integer(1);
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int qwerty = 0;
        list.delete(0, list.length());
        documentIDs.clear();
        docs.clear();
        inDocumentFreq.clear();
        for (IntWritable val : values) {
      	  qwerty++;
      	  Integer docID = Integer.valueOf(val.toString());
      	  if(!docs.contains(docID)){
      		  documentIDs.add(docID);
      		  docs.add(docID);
      		  sum++;
      	  }
      	  Integer docfreq = inDocumentFreq.get(docID);
      	  
      	  if(docfreq != null){
      		  inDocumentFreq.put(docID, docfreq + 1);
      	  }else {
      		  inDocumentFreq.put(docID, one);
      	  }
        }
        Object[] temp = documentIDs.toArray();
        Arrays.sort(temp);
        list.append(sum);
        for(int i = 0; i<documentIDs.size(); i++){
      	  list.append(" ").append(temp[i]).append(" ").append(inDocumentFreq.get((Integer)temp[i]));
        }
        result.set(list.toString());
        context.write(key, result);
      }
    }

  public static void main(String[] args) throws Exception {

	  Configuration conf = new Configuration ( ) ;
	  conf.set("xmlinput.start", "<page>"); 
	  conf.set("xmlinput.end", "</page>");
	  conf.set(
			  	"io.serializations",
	  			"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	  

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "indexdocs");

    //FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/data"));
    //FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/index_docs_mark/"));
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    //FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/data/"));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    //FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/indexdocs3/"));
    
    job.setInputFormatClass(XmlInputFormat.class);
    job.setJarByClass(IndexerDocs.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
