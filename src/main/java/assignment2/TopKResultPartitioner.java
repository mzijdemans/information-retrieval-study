package assignment2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopKResultPartitioner extends Partitioner<TopKResultsKey, Writable> {

	public int getPartition(TopKResultsKey key, Writable value, int numPartitions) {
		return Math.abs(key.getQueryId().hashCode() ) % numPartitions;
	}
}