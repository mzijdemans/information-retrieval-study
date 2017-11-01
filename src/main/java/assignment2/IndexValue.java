package assignment2;

import java.io.*;
import java.util.Map;

import org.apache.hadoop.io.*;

public class IndexValue implements WritableComparable<IndexValue>{

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(IndexValue arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
}
//	private IntWritable docWeight;
//	private Map<IntWritable, IntWritable> qs;
//	/**
//	 * Constructor
//	 * @param t
//	 * @param id
//	 */
//	public IndexKey(int id, String t){
//		set(new IntWritable(id), new Text(t));
//	}
//	
//	public IndexKey(IntWritable id, Text t){
//		set(id, t);
//	}
//	
//	public void set(IntWritable id, Text t){
//		this.term = t;
//		this.docid = id;
//	}
//	
//	public void set(int id, String t){
//		this.term.set(t);
//		this.docid.set(id);
//	}
//
//	/**
//	 * @return the term
//	 */
//	public Text getTerm() {
//		return term;
//	}
//
//	/**
//	 * @return the docid
//	 */
//	public IntWritable getDocid() {
//		return docid;
//	}
//
//	public void write(DataOutput out) throws IOException {
//		this.docid.write(out);
//		this.term.write(out);
//	}
//	
//	public void readFields(DataInput in) throws IOException {
//		this.docid.readFields(in);
//		this.term.readFields(in);
//	}
//	
//	public int compareTo(IndexKey other){
//		if(this.docid.compareTo(other.getDocid()) < 0){
//			return -1;
//		}
//		else if(this.docid.compareTo(other.getDocid()) == 0){
//			return this.term.compareTo(other.getTerm());
//		}
//		else{
//			return 1;
//		}
//	}
//	
//	public boolean equals(IndexKey other){
//		return this.docid.equals(other.getDocid());
//	}
//	
//	public String toString(){
//		return this.docid+" "+this.term;
//	}
//
//	@Override
//	public int compareTo(IndexValue arg0) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//}
