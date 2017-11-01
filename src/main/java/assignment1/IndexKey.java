package assignment1;

import java.io.*;
import org.apache.hadoop.io.*;

public class IndexKey implements WritableComparable<IndexKey>{
	private Text term;
	private IntWritable docid;
	/**
	 * Constructor
	 * @param t
	 * @param id
	 */
	public IndexKey(String t, int id){
		set(new Text(t), new IntWritable(id));
	}
	
	public IndexKey(Text t, IntWritable id){
		set(t, id);
	}
	
	public void set(Text t, IntWritable id){
		this.term = t;
		this.docid = id;
	}
	
	public void set(String t, int id){
		this.term.set(t);
		this.docid.set(id);
	}
	
	public IndexKey(){
		this.term = new Text();
		this.docid = new IntWritable();
	}

	/**
	 * @return the term
	 */
	public Text getTerm() {
		return term;
	}

	/**
	 * @return the docid
	 */
	public IntWritable getDocid() {
		return docid;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.term.write(out);
		this.docid.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.term.readFields(in);
		this.docid.readFields(in);
		//this.term = in.readLine();
		//this.docid = in.readInt();
		//this.term = s[0];
		//this.docid = Integer.getInteger(s[1]);
		
	}
	
	public int compareTo(IndexKey other){
		if(this.term.compareTo(other.getTerm()) < 0){
			return -1;
		}
		else if(this.term.compareTo(other.getTerm()) == 0){
			return this.docid.get() - other.getDocid().get();
		}
		else{
			return 1;
		}
	}
	
	public boolean equals(IndexKey other){
		return this.term.equals(other.getTerm());
	}
	
	public String toString(){
		return this.term+ " " +this.docid;
	}

}
