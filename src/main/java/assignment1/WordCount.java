package assignment1;

import java.io.IOException;
import java.util.HashMap;
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

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
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

        		  context.write(new Text("!!!!!Total: "), one);
  
    }
    private static HashMap<String, String> setStopWords(){
    	HashMap<String, String> stopwords = new HashMap<String, String>(30);  
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
		  return stopwords;
    }
  }
  
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
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
    Job job = new Job(conf, "word count");

    //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    //amazone path
    FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/data/"));
    
    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    //amazone path
    FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/doccount/"));
    job.setInputFormatClass(XmlInputFormat.class);
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
