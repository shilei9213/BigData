package x.bd.hadoop.job.template;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区
 * 
 * @author shilei
 * 
 */
public class TPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		return 0;
	}
}
