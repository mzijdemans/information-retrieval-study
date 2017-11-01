package assignment2;

import java.util.ArrayList;

public class TopXTerms {
	private ArrayList<String> terms;
	private ArrayList<Double> frequency;
	private int size;
	
	public TopXTerms(int size){
		terms = new ArrayList<String>(size);
		frequency = new ArrayList<Double>(size);
		this.size = size;
		for(int index = 0; index < this.size; index++){
			terms.add(index, "");
			frequency.add(index, 0.0);
		}
	}
	
	public void add(String term, double fr){
		double lowest = this.frequency.get(0);
		int index_lowest = 0;
		for(int index = 1; index < this.size; index++){
			if(frequency.get(index).doubleValue() < lowest){
				index_lowest = index;
				lowest = this.frequency.get(index);
			}
		}
		if(frequency.get(index_lowest).doubleValue() < fr){
			terms.add(index_lowest, term);
			frequency.add(index_lowest, fr);
		}
	}
	
	public void add(String term, Double fr){
		for(int index = 0; index < this.size; index++){
			if(frequency.get(index).doubleValue() < fr.doubleValue()){
				terms.add(index, term);
				frequency.add(index, fr);
				break;
			}
		}
	}
	
	public void clear(){
		for(int index = 0; index < this.size; index++){
			terms.add(index, "");
			frequency.add(index, 0.0);
		}
	}
	
	public String toString(){
		String result = new String();
		for(int index = 0; index < this.size; index++){
			result += terms.get(index) + " ";
		}
		result = result.substring(0, result.length()-1);
		return result;
	}
}
