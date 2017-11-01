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

public class GetTopKResults {

	public static class GetTopKResultsMapper extends
			Mapper<Object, Text, TopKResultsKey, IntWritable> {

		private String temp = "";
		private Text queryId = new Text();
		private IntWritable docId = new IntWritable();
		private DoubleWritable score = new DoubleWritable();
		private TopKResultsKey resultKey = new TopKResultsKey();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				StringTokenizer str = new StringTokenizer(value.toString());
				this.temp = str.nextToken();
				this.queryId.set(this.temp);
				this.temp = str.nextToken();
				this.docId.set(Integer.parseInt(this.temp));
				this.temp = str.nextToken();
				this.score.set(Double.parseDouble(this.temp));
				this.resultKey.set(this.queryId, this.score);
				context.write(this.resultKey, this.docId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class GetTopKResultsReducer extends
			Reducer<TopKResultsKey, IntWritable, Text, Text> {

		private String lastQueryId = null;
		private int counter = 1;
		private Text result = new Text();

		public void reduce(TopKResultsKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			if (!key.getQueryId().toString().equals(lastQueryId)) {
				// new query!
				// reset counter
				this.counter = 1;
			}
			for (IntWritable value : values) {
				if (this.counter <= 1000) {
					// emit
					/*
					 * 60-1 Q0 243891 1 527.257 Exp which is [queryID] [Q0]
					 * [docid] [rank] [score] [Exp] (the two "dummy" elements Q0
					 * and Exp are always the same)
					 */
					result.set("Q0 " + value.toString() + " " + counter + " "
							+ key.getScore().toString() + " " + "STANDARD");
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
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    //amazone path
	    //FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/data/"));
	    
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    //amazone path
	    //FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/doccount/"));
		//FileInputFormat.addInputPath(job, new Path("wordcount_wiki/output_search7"));
		//FileOutputFormat.setOutputPath(job, new Path("wordcount_wiki/output_top1000_notFiltered"));
		job.setJarByClass(GetTopKResults.class);
		job.setMapperClass(GetTopKResultsMapper.class);
		job.setReducerClass(GetTopKResultsReducer.class);
		job.setOutputKeyClass(TopKResultsKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(TopKResultPartitioner.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
