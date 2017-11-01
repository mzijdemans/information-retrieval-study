package assignment2;

import java.io.*;
import org.apache.hadoop.io.*;

public class TopKResultsKey implements WritableComparable<TopKResultsKey>{
	private Text queryId;
	private DoubleWritable score;
	/**
	 * Constructor
	 * @param t
	 * @param id
	 */
	public TopKResultsKey(String q, double sc){
		set(new Text(q), new DoubleWritable(sc));
	}
	
	public TopKResultsKey(Text q, DoubleWritable sc){
		set(q, sc);
	}
	
	public void set(Text q, DoubleWritable sc){
		this.queryId = q;
		this.score = sc;
	}
	
	public void set(String q, double sc){
		this.queryId.set(q);
		this.score.set(sc);
	}
	
	public TopKResultsKey(){
		this.queryId = new Text();
		this.score = new DoubleWritable();
	}

	public Text getQueryId() {
		return this.queryId;
	}

	public DoubleWritable getScore() {
		return this.score;
	}

	public void write(DataOutput out) throws IOException {
		this.queryId.write(out);
		this.score.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.queryId.readFields(in);
		this.score.readFields(in);
	}
	
	public int compareTo(TopKResultsKey other){
		if(this.queryId.compareTo(other.getQueryId()) < 0){
			return -1;
		}
		else if(this.queryId.compareTo(other.getQueryId()) == 0){
			return other.getScore().compareTo(this.score);
		}
		else{
			return 1;
		}
	}
	
	public boolean equals(TopKResultsKey other){
		return this.queryId.equals(other.getQueryId());
	}
	
	public String toString(){
		return this.queryId+" "+this.score;
	}

}
