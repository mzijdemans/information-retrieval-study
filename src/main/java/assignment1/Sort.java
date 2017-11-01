package assignment1;

import java.io.IOException;
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

public class Sort {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    
    
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      int num = 0;
      String temp = "";
      while(itr.hasMoreTokens()){
    	  String next = itr.nextToken();
    	  if(num == 0){
    		  temp = next;
    		  num++;
    	  }
    	  else{
    		  String t= "";
    		  for(int i = 0; i<20;i++){
    			  if(next.length() < i){
    				  t += "0"; 
    			  }
    		  }
    		  temp = t+next + " " + temp;
    		  context.write(new Text(temp), one);
    		  num = 0;
    	  }
      }
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
	  conf.set(
			  	"io.serializations",
	  			"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
	  

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "sort");

    //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/filter2/"));
    //FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/filter/"));
    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/sortfilter2/"));
    //FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/sortfilter/"));
    job.setJarByClass(Sort.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
