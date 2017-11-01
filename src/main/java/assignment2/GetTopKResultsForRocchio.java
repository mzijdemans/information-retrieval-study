package assignment2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GetTopKResultsForRocchio {

	public static class GetTopKResultsMapper extends
			Mapper<Object, Text, TopKResultsKey, Text> {

		private String temp = "";
		private Text queryId = new Text();
		private Text docId = new Text();
		private DoubleWritable score = new DoubleWritable();
		private TopKResultsKey resultKey = new TopKResultsKey();
		
		private String text;
		private StringBuffer resultBuffer = new StringBuffer();
		private String space = " ";

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				StringTokenizer str = new StringTokenizer(value.toString());
				this.resultBuffer.setLength(0);
				
				this.temp = str.nextToken();
				this.queryId.set(this.temp);
				this.temp = str.nextToken();
				this.docId.set(this.temp);
				this.temp = str.nextToken();
				this.score.set(Double.parseDouble(this.temp));
				this.resultKey.set(this.queryId, this.score);
				
				while(str.hasMoreTokens()){ // remember the terms
					this.text = str.nextToken();
					this.resultBuffer.append(space).append(this.text);
				}
				
				this.docId.set(this.docId.toString() + this.resultBuffer.toString());
				context.write(this.resultKey, this.docId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class GetTopKResultsReducer extends
			Reducer<TopKResultsKey, Text, Text, Text> {

		private String lastQueryId = null;
		private int counter = 1;
		private Text result = new Text();
		private String text;

		public void reduce(TopKResultsKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			if (!key.getQueryId().toString().equals(lastQueryId)) {
				// new query!
				// reset counter
				this.counter = 1;
			}
			for (Text value : values) {
				if (this.counter <= 20) { // TODO TOP k for rochio we choose 20 documents
					// emit
					/*
					 * [queryID] [docid] [score] [terms...]
					 */
					text = value.toString();
					String[] list = text.split(" ");
					int index = text.indexOf(" ");
					if(index > -1){
						text = text.substring(index, text.length());
						result.set("" + list[0] + " " + key.getScore().toString() + " " + text);
					}else{
						result.set("" + list[0] + " " + key.getScore().toString());
					}
					context.write(key.getQueryId(), result);
				} else {
					// do nothing
				}
				this.counter++;
				lastQueryId = key.getQueryId().toString();
			}
		}

		public void close(Context context) throws IOException,
				InterruptedException {

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(
						"io.serializations",
						"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		Job job = new Job(conf, "searchEngine");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	   // FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    //amazone path
	    //FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/data/"));
	    
	    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    //amazone path
	    //FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/doccount/"));
		FileInputFormat.addInputPath(job, new Path("wordcount_wiki/output_search_x_most_freq_terms_1"));
		FileOutputFormat.setOutputPath(job, new Path("wordcount_wiki/output_top20_Filtered"));
		job.setJarByClass(GetTopKResultsForRocchio.class);
		job.setMapperClass(GetTopKResultsMapper.class);
		job.setReducerClass(GetTopKResultsReducer.class);
		job.setOutputKeyClass(TopKResultsKey.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(TopKResultPartitioner.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
