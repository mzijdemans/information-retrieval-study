package assignment1;

import java.io.IOException;
import java.util.HashMap;
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

public class OptimizedIndexerDocs {

  public static class TokenizerMapper extends Mapper<Object, Text, IndexKey, IntWritable>{
    
    
    private static HashMap<String, String> stopwords = setStopWords();
    
    private boolean blockParsingTranslations = false;
    private Stemmer stemmer = new Stemmer();
    private HashMap<String, Integer> terms = new HashMap<String, Integer>();
    private String id = "";
    private String text = "";
    StringBuffer next = new StringBuffer();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
      DocumentBuilderFactory dbf =
          DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(value.toString()));

      Document doc = db.parse(is);
      NodeList nodes = doc.getElementsByTagName("page");
      
      terms.clear();
      Element element = (Element) nodes.item(0);
      id = element.getElementsByTagName("id").item(0).getTextContent();    
      text = element.getElementsByTagName("text").item(0).getTextContent();

      StringTokenizer str = new StringTokenizer(text);
      while(str.hasMoreTokens()){
    	  next.setLength(0);
    	  next.append(str.nextToken().toLowerCase());
    	  boolean skip = false;
    	  
    	  //[[an:Intelichencia artificial]] for example
    	  if(next.length() > 2 && (this.blockParsingTranslations || (next.indexOf(":") != -1 && next.substring(0, 2).equals("[[")))){
    		  this.blockParsingTranslations = true;
    		  if(next.substring(next.length()-2, next.length()).equals("]]")){ //end of translation link (wikipedia)
    			  this.blockParsingTranslations = false;
    		  }
    	  }else{
    		  String s = next.toString().replaceAll("[^\\p{Alpha}]+","");
    		  next.setLength(0);
    		  next.append(s);
	          if(s.matches("[a-z]+")){ //[A-Z]* incase casefolding is turned off...
		        	  skip = false;
		          }else{
		        	  skip = true;
		          }
	    	  if(!skip){
	    		  if(stopwords.containsKey(next.toString())){
	    			  skip = true;
	    		  }
		      }
        	  if(!skip){
        		  stemmer.add(next.toString().toCharArray(), next.length());
        		  stemmer.stem();
        		  next.replace(0, next.length(), stemmer.toString());
        	  }
        	  if(!skip){
        		  if(terms.containsKey(next.toString())){
        			  terms.put(next.toString(), terms.get(next.toString()) + 1);
        		  }
        		  else{
        			  terms.put(next.toString(),1);  
        		  }
        	  }
    	  }
      }
      String k;
      Integer v;
      IndexKey ik = new IndexKey();
      IntWritable val = new IntWritable();
      for (Map.Entry<String, Integer> entry : terms.entrySet()) {
    	    k = entry.getKey();
    	    v = entry.getValue();
    	    ik.set(k, (int)Integer.valueOf(id));
    	    val.set(v);
    	    context.write(ik, val);
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
  
public static class IndexReducer extends Reducer<IndexKey,IntWritable,Text,Text> {
	private String last = "";
	private StringBuffer postings = new StringBuffer();
	private int count = 0;
	private String space = " ";
	public void reduce(IndexKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for (IntWritable val : values) {
			if(!last.equals(key.getTerm().toString())){
	    		if(last != "")
	    			context.write(new Text(last), new Text(count+space+postings.toString()));
	    		last = key.getTerm().toString();
	    		//postings.delete(0, postings.capacity());
	    		postings.setLength(0);
	    		postings.append(key.getDocid());
	    		postings.append(space);
	    		postings.append(val);
	    		count = 1;
	    	}
	    	else{
	    		postings.append(space);
	    		postings.append(key.getDocid());
	    		postings.append(space);
	    		postings.append(val);
	    		count++;
	    	}
		}
	}
	public void close(Context context) throws IOException, InterruptedException{
		context.write(new Text(last), new Text(count+space+postings.toString()));
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
    
    //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/data/"));
    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/docindex/"));
    
    job.setInputFormatClass(XmlInputFormat.class);
    job.setJarByClass(OptimizedIndexerDocs.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setPartitionerClass(IndexPartitioner.class);
    job.setReducerClass(IndexReducer.class);
    job.setOutputKeyClass(IndexKey.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
