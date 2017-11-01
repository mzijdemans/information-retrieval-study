package assignment2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NewQueriesRocchio {

	public static class NewQueriesRocchioMapper extends
			Mapper<Object, Text, Text, Text> {

		private int rocchio = 5; // the value from M...
		private String text = new String();
		private int counter = 0;
		private StringBuffer resultBuffer = new StringBuffer();
		
		private Text resultKey = new Text();
		private Text terms = new Text();
		private String space = " ";

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				// read >> QueryNo Doc_id score terms...
				StringTokenizer str = new StringTokenizer(value.toString());
				this.resultBuffer.setLength(0);
				this.counter = 0;
				//get query
				this.resultKey.set(str.nextToken());
				//skip next 2
				str.nextToken();
				str.nextToken();
				while(str.hasMoreTokens()){
					if(counter < this.rocchio){
						this.text = str.nextToken();
						this.resultBuffer.append(space).append(this.text);
					}else{
						break;
					}
					this.counter++;
				}
				this.terms.set(resultBuffer.toString());
				// emit query id, term_list
				context.write(this.resultKey, this.terms);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static class NewQueriesRocchioReducer extends
			Reducer<Text, Text, Text, Text> {
		
		private StringBuffer resultBuffer = new StringBuffer();
		private String space = " ";
		private Text result = new Text(); 
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			resultBuffer.setLength(0);
			for(Text value: values){
				resultBuffer.append(space).append(value.toString());
			}
			result.set(resultBuffer.toString());
			context.write(key, result);
		}

		public void close(Context context) throws IOException,
				InterruptedException {

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf
				.set(
						"io.serializations",
						"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		Job job = new Job(conf, "searchEngine");

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// amazone path
	//	FileInputFormat.addInputPath(job, new Path(
		//		"s3://in4325-enwiki/output/be_ka_zi/docindex/"));

		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// amazone path
		//FileOutputFormat.setOutputPath(job, new Path(
		//		"s3://in4325-enwiki/output/be_ka_zi/docindex_searchresult3/"));
//		FileInputFormat.addInputPath(job, new Path("wordcount_wiki/output_index1"));
		FileInputFormat.addInputPath(job, new Path("wordcount_wiki/output_top20_Filtered"));
		FileOutputFormat.setOutputPath(job, new Path("wordcount_wiki/output_test_new_limit5_queries_2"));
		// job.setInputFormatClass(XmlInputFormat.class);
		job.setJarByClass(NewQueriesRocchio.class);
		job.setMapperClass(NewQueriesRocchioMapper.class);
		job.setCombinerClass(NewQueriesRocchioReducer.class);
		job.setReducerClass(NewQueriesRocchioReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setPartitionerClass(IndexPartitioner.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
