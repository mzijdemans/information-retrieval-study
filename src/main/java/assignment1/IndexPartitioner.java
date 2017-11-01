package assignment1;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class IndexPartitioner extends Partitioner<IndexKey, Writable> {

	public int getPartition(IndexKey key, Writable value, int numPartitions) {
		return Math.abs(key.getTerm().hashCode()) % numPartitions;
	}
}