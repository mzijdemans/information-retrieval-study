package assignment2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

public class SearchEngine {

  public static class TokenizerMapper extends Mapper<Object, Text, IndexKey, Text>{
    
	private IndexKey indexKey = new IndexKey();
	private Text valuesText = new Text();
	private StringBuffer queries = new StringBuffer();
    StringBuffer term = new StringBuffer();
    private double docCount = 0;
    private Double queryWeight;
    static List<Query> queriesList = setQueries();
	// Queries with no filters
    //static List<Query> queriesList = setQueriesNoFiter();

    private static List<Query> setQueriesNoFiter() {
    	List<Query> queriesList = new ArrayList<Query>();
    	List<String> list = Arrays.asList("Harry","Potter","Quidditch","Gryffindor","character");
    	queriesList.add(new Query(list,"104-1"));
    	list = Arrays.asList("I","am","looking","for","characters","in","the","Harry","Potter","universe","that","are","part","of","Gryffindor","house","or","team","and","that","play","Quidditch");
    	queriesList.add(new Query(list,"104-2"));
    	list = Arrays.asList("Nobel","Prize","in","Literature","winners","who","were","also","poets");
    	queriesList.add(new Query(list,"110-1"));
    	list = Arrays.asList("Formula","1","drivers","that","won","the","Monaco","Grand","Prix");
    	queriesList.add(new Query(list,"113-1"));
    	list = Arrays.asList("I","want","a","list","of","Formula","1","drivers.","Each","of","them","must","have","won","at","least","once","the","Monaco","Grand","Prix","held","in","Monte","Carlo.");
    	queriesList.add(new Query(list,"113-2"));
    	list = Arrays.asList("The","Monaco","Grand","Prix","(GP)","is","a","Formula","1","competition","held","in","Monte","Carlo","since","1929.","I","want","to","find","all","the","drivers","that","won","the","GP","over","the","years.");
    	queriesList.add(new Query(list,"113-3"));
    	list = Arrays.asList("Formula","One","World","Constructors'","Champions");
    	queriesList.add(new Query(list,"115-1"));
    	list = Arrays.asList("I","want","the","list","of","Formula","One","(F1)","teams","that","won","at","least","one","World","Constructors'","championship");
    	queriesList.add(new Query(list,"115-2"));
    	list = Arrays.asList("I","want","a","list","of","people","who","won","were","nobel","prize","laureates","in","any","field","and","have","Italian","nationality.");
    	queriesList.add(new Query(list,"116-3"));
    	list = Arrays.asList("French","car","models","in","1960's");
    	queriesList.add(new Query(list,"118-1"));
    	list = Arrays.asList("Swiss","cantons","where","they","speak","German");
    	queriesList.add(new Query(list,"119-1"));
    	list = Arrays.asList("I","want","to","find","the","cantons","of","Switzerland","where","German","is","one","of","the","official","language","of","the","canton.");
    	queriesList.add(new Query(list,"119-2"));
    	list = Arrays.asList("Switzerland","is","a","federal","republic","composed","of","26","cantons.","Each","canton","has","one","or","more","official","language:","German,","French,","Italian","and/or","Romansh.","I","am","looking","for","the","list","of","the","Swiss","cantons","where","German","is","an","official","language.");
    	queriesList.add(new Query(list,"119-3"));
    	list = Arrays.asList("FIFA","world","cup","national","team","winners","since","1974");
    	queriesList.add(new Query(list,"123-1"));
    	list = Arrays.asList("Novels","that","won","the","Booker","Prize");
    	queriesList.add(new Query(list,"124-1"));
    	list = Arrays.asList("Find","articles","that","describe","the","novels","which","won","the","prestigious","booker","prize.");
    	queriesList.add(new Query(list,"124-3"));
    	list = Arrays.asList("toy","train","manufacturers","that","are","still","in","business");
    	queriesList.add(new Query(list,"126-1"));
    	list = Arrays.asList("german","female","politicians");
    	queriesList.add(new Query(list,"127-1"));
    	list = Arrays.asList("Bond","girls");
    	queriesList.add(new Query(list,"128-1"));
    	list = Arrays.asList("I","want","the","names","of","the","actresses","that","have","played","the","role","as","James","Bond","girl","in","any","of","the","James","Bond","movies.");
    	queriesList.add(new Query(list,"128-2"));
    	list = Arrays.asList("Star","Trek","Captains");
    	queriesList.add(new Query(list,"130-1"));
    	list = Arrays.asList("I","want","a","list","of","names","that","correspond","to","the","starship","captains","from","the","Star","Trek","series.");
    	queriesList.add(new Query(list,"130-2"));
    	list = Arrays.asList("record-breaking","sprinters","in","male","100-meter","sprints");
    	queriesList.add(new Query(list,"134-1"));
    	list = Arrays.asList("professional","baseball","team","in","Japan");
    	queriesList.add(new Query(list,"135-1"));
    	list = Arrays.asList("Japanese","players","in","Major","League","Baseball");
    	queriesList.add(new Query(list,"136-1"));
    	list = Arrays.asList("National","Parks","East","Coast","Canada","US");
    	queriesList.add(new Query(list,"138-1"));
    	list = Arrays.asList("List","national","parks,","forest,","and","mountains","near","the","East","Coast","of","Canada","and","the","United","States,","located","around","Montreal","and","Boston");
    	queriesList.add(new Query(list,"138-2"));
    	list = Arrays.asList("Films","directed","by","Akira","Kurosawa");
    	queriesList.add(new Query(list,"139-1"));
    	list = Arrays.asList("Airports","in","Germany");
    	queriesList.add(new Query(list,"140-1"));
    	list = Arrays.asList("Universities","in","Catalunya");
    	queriesList.add(new Query(list,"141-1"));
    	list = Arrays.asList("German","cities","that","have","been","part","of","the","hanseatic","league","in","Rhine,","Westphalia,","the","Netherlands","Circle");
    	queriesList.add(new Query(list,"143-2"));
    	list = Arrays.asList("chess","world","champions");
    	queriesList.add(new Query(list,"144-1"));
    	return queriesList;
    }

    private static List<Query> setQueries() {
    	List<Query> queriesList = new ArrayList<Query>();
    	List<String> list = Arrays.asList("harri","potter","quidditch","gryffindor","charact");
    	queriesList.add(new Query(list,"104-1"));
    	list = Arrays.asList("am","look","charact","harri","potter","univers","part","gryffindor","hous","team","plai","quidditch");
    	queriesList.add(new Query(list,"104-2"));
    	list = Arrays.asList("nobel","prize","literatur","winner","were","also","poet");
    	queriesList.add(new Query(list,"110-1"));
    	list = Arrays.asList("formula","driver","won","monaco","grand","prix");
    	queriesList.add(new Query(list,"113-1"));
    	list = Arrays.asList("want","list","formula","driver","each","them","must","won","least","onc","monaco","grand","prix","held","mont","carlo");
    	queriesList.add(new Query(list,"113-2"));
    	list = Arrays.asList("monaco","grand","prix","gp","formula","competit","held","mont","carlo","sinc","want","find","all","driver","won","gp","over","year");
    	queriesList.add(new Query(list,"113-3"));
    	list = Arrays.asList("formula","on","world","constructor","champion");
    	queriesList.add(new Query(list,"115-1"));
    	list = Arrays.asList("want","list","formula","on","f","team","won","least","on","world","constructor","championship");
    	queriesList.add(new Query(list,"115-2"));
    	list = Arrays.asList("want","list","peopl","won","were","nobel","prize","laureat","ani","field","italian","nation");
    	queriesList.add(new Query(list,"116-3"));
    	list = Arrays.asList("french","car","model","s");
    	queriesList.add(new Query(list,"118-1"));
    	list = Arrays.asList("swiss","canton","where","speak","german");
    	queriesList.add(new Query(list,"119-1"));
    	list = Arrays.asList("want","find","canton","switzerland","where","german","on","offici","languag","canton");
    	queriesList.add(new Query(list,"119-2"));
    	list = Arrays.asList("switzerland","feder","republ","compos","canton","each","canton","ha","on","more","offici","languag","german","french","italian","andor","romansh","am","look","list","swiss","canton","where","german","offici","languag");
    	queriesList.add(new Query(list,"119-3"));
    	list = Arrays.asList("fifa","world","cup","nation","team","winner","sinc");
    	queriesList.add(new Query(list,"123-1"));
    	list = Arrays.asList("novel","won","booker","prize");
    	queriesList.add(new Query(list,"124-1"));
    	list = Arrays.asList("find","articl","describ","novel","won","prestigi","booker","prize");
    	queriesList.add(new Query(list,"124-3"));
    	list = Arrays.asList("toi","train","manufactur","still","busi");
    	queriesList.add(new Query(list,"126-1"));
    	list = Arrays.asList("german","femal","politician");
    	queriesList.add(new Query(list,"127-1"));
    	list = Arrays.asList("bond","girl");
    	queriesList.add(new Query(list,"128-1"));
    	list = Arrays.asList("want","name","actress","plai","role","jame","bond","girl","ani","jame","bond","movi");
    	queriesList.add(new Query(list,"128-2"));
    	list = Arrays.asList("star","trek","captain");
    	queriesList.add(new Query(list,"130-1"));
    	list = Arrays.asList("want","list","name","correspond","starship","captain","star","trek","seri");
    	queriesList.add(new Query(list,"130-2"));
    	list = Arrays.asList("recordbreak","sprinter","male","meter","sprint");
    	queriesList.add(new Query(list,"134-1"));
    	list = Arrays.asList("profession","basebal","team","japan");
    	queriesList.add(new Query(list,"135-1"));
    	list = Arrays.asList("japanes","player","major","leagu","basebal");
    	queriesList.add(new Query(list,"136-1"));
    	list = Arrays.asList("nation","park","east","coast","canada");
    	queriesList.add(new Query(list,"138-1"));
    	list = Arrays.asList("list","nation","park","forest","mountain","near","east","coast","canada","unit","state","locat","around","montreal","boston");
    	queriesList.add(new Query(list,"138-2"));
    	list = Arrays.asList("film","direct","akira","kurosawa");
    	queriesList.add(new Query(list,"139-1"));
    	list = Arrays.asList("airport","germani");
    	queriesList.add(new Query(list,"140-1"));
    	list = Arrays.asList("univers","catalunya");
    	queriesList.add(new Query(list,"141-1"));
    	list = Arrays.asList("german","citi","been","part","hanseat","leagu","rhine","westphalia","netherland","circl");
    	queriesList.add(new Query(list,"143-2"));
    	list = Arrays.asList("chess","world","champion");
    	queriesList.add(new Query(list,"144-1"));
    	return queriesList;

    }

    private String space = " ";
    private IntWritable keyDocId = new IntWritable(0);
    private Text keyTerm = new Text();
    private int doc_id;
    private double in_doc_freq;
    private double idf;
    private double docWeight;
    private int freq;
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
	      StringTokenizer str = new StringTokenizer(value.toString());
	      term.setLength(0);
	      term.append(str.nextToken()); // get the term
	      docCount = Double.parseDouble(str.nextToken()); // get the number of the files that the term is present
	      //large corpus = 7649051
	      idf = Math.log10(7649051.0 / this.docCount);
	      while(str.hasMoreTokens()) {
	    	  doc_id = Integer.parseInt(str.nextToken());
	    	  in_doc_freq = Double.parseDouble(str.nextToken());
	    	  //To Normalize
	    	  //in_doc_freq = Math.log10(in_doc_freq);
	    	  
	    	  //emit
	    	  // 1. document weight
	    	  docWeight = in_doc_freq * idf;
	    	  queries.setLength(0);
	    	  // 2. weight for every query
	    	  for(Query query: queriesList) {
	    		  freq = Collections.frequency(query.terms, term.toString());
	    		  if(freq != 0) {
	    			  //To Normalize
		    		  //queryWeight =  Math.log10(freq) * idf;
		    		  queryWeight =  freq * idf;
		    		  queries.append(query.queryNo).append(space).append(queryWeight).append(space);
	    		  }
	    	  }
	    	  if(queries.length() != 0) {
		    	  keyDocId.set(doc_id);
		    	  keyTerm.set(term.toString());
		    	  this.indexKey.set(keyDocId, keyTerm);
		    	  this.valuesText.set(docWeight + space + in_doc_freq + space + queries.toString());
		    	  context.write(this.indexKey, this.valuesText);
	    	  }else{
	    		  keyDocId.set(doc_id);
		    	  keyTerm.set(term.toString());
		    	  this.indexKey.set(keyDocId, keyTerm);
		    	  this.valuesText.set(docWeight + space + in_doc_freq);
		    	  context.write(this.indexKey, this.valuesText);
	    	  }
	      }
      }
      catch (Exception e) {
          e.printStackTrace();
      }
    }
    
  }
  
  public static class IndexReducer extends Reducer<IndexKey,Text,Text,Text> {
	private Integer lastDocId = new Integer(-1);
	private double docWeight = 0.0;
	private Double score = new Double(0.0);
	private double finalScore = 0.0;
	private double sumOfweights = 0.0;
	private Map<String,Double> documentScore = new HashMap<String, Double>();
	private String[] valueTerms;
	private Map<String, Double> queryWeights = new HashMap<String, Double>();
	
	private TopXTerms TermFrequencies = new TopXTerms(20); // count of most occuring terms
	
	private String space = " ";
    private Text resultQNo = new Text();
    private Text resultDocIDfScr = new Text();
    
    private Integer minusOne = new Integer(-1);
	
	public void reduce(IndexKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if(lastDocId != key.getDocid().get()) {
			if(!lastDocId.equals(minusOne)) {
				// We have a new doc id we have to emit the information for the previous one
				// We have to emit the information per document id and the final score for each query
				for(String queryNo: queryWeights.keySet()) {
					finalScore = (double)(documentScore.get(queryNo) / ((double) Math.sqrt(sumOfweights)));
					resultQNo.set(queryNo);
					resultDocIDfScr.set(lastDocId.toString() + space + finalScore + space + TermFrequencies.toString());
					context.write(resultQNo, resultDocIDfScr);
				}
				
			}
			// variables initialization
			sumOfweights = 0.0;
			documentScore.clear();
			TermFrequencies.clear(); // clear the term list, new doc_id
			lastDocId = key.getDocid().get();
    	}
		queryWeights.clear();
		for (Text value : values) {
			valueTerms = value.toString().split(" ");
			for(int i = 2; i < valueTerms.length; i+=2){
				// extract queryNumber and queryWeight for the given term
				queryWeights.put(valueTerms[i], Double.parseDouble(valueTerms[i+1]));
    		}
			// Calculations for the final results
    		docWeight = Double.parseDouble(valueTerms[0]);
    		TermFrequencies.add(key.getTerm().toString(), Double.parseDouble(valueTerms[1])); // add score and term to list 
    		// calculate the squares
    		sumOfweights += Math.pow(docWeight, 2.0);
    		for(String queryNo: queryWeights.keySet()) {
    			score = documentScore.get(queryNo);
    			if(score != null)
    				score += (Double) docWeight*queryWeights.get(queryNo);
    			else
    				score = (Double) docWeight*queryWeights.get(queryNo);
    			documentScore.put(queryNo, score);
    		}
		}
	}
	public void close(Context context) throws IOException, InterruptedException{
		for(String queryNo: queryWeights.keySet()) {
			finalScore = (double)(documentScore.get(queryNo) / ((double) Math.sqrt(sumOfweights)));
			resultQNo.set(queryNo);
			resultDocIDfScr.set(lastDocId.toString() + space + finalScore + space + TermFrequencies.toString());
			context.write(resultQNo, resultDocIDfScr);
		}
	}
}
  
  public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration ( ) ;
	conf.set("xmlinput.start", "<page>"); 
	conf.set("xmlinput.end", "</page>");
	conf.set(
			  	"io.serializations",
	"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

	Job job = new Job(conf, "searchEngine");
	
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    //amazone path
    FileInputFormat.addInputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/docindex/"));
    
    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    //amazone path
    FileOutputFormat.setOutputPath(job, new Path("s3://in4325-enwiki/output/be_ka_zi/docindex_searchresult3/"));
	//FileInputFormat.addInputPath(job, new Path("wordcount_wiki/output_index1"));
    //FileOutputFormat.setOutputPath(job, new Path("wordcount_wiki/output_search_x_most_freq_terms_1"));
	//job.setInputFormatClass(XmlInputFormat.class);
	job.setJarByClass(SearchEngine.class);
	job.setMapperClass(TokenizerMapper.class);
	//job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IndexReducer.class);
	job.setOutputKeyClass(IndexKey.class);
	job.setOutputValueClass(Text.class);
	job.setPartitionerClass(IndexPartitioner.class);
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
