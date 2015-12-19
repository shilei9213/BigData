package x.bd.hadoop.job.imputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 自定义 InputStream , 产生一组随机数，求最大值
 * 
 * @author shilei
 * 
 */
public class FindMaxValueInputFormat extends
		InputFormat<IntWritable, IntWritable> {

	/**
	 * 根据换进信息，获取最分片列表,每一个分片由一个maptask执行
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		// 1 初始化要处理的数据
		int numSize = context.getConfiguration().getInt("num.size", 100);
		int[] numArray = new int[numSize];
		for (int i = 0; i < numSize; i++) {
			numArray[i] = (int) (Math.random() * numSize);
		}

		// 2 根据用户指定的map数分割数据为InputSplit列表
		int maps = context.getConfiguration().getInt("mapred.map.tasks", 2);
		int length = (int) Math.floor(numSize / maps);

		List<InputSplit> splits = new ArrayList<InputSplit>();
		FindMaxValueInputSplit split = null;
		int start = 0;
		int end = 0;

		for (int i = 0; i < maps; i++) {
			if (i != maps - 1) {
				// 常规块
				end = start + length - 1;

			} else {
				// 最末块
				end = numArray.length - 1;
			}

			split = new FindMaxValueInputSplit(start, end, numArray);
			splits.add(split);
			start = end + 1;
		}
		return splits;
	}

	/**
	 * 根据分片信息及环境获取记录读取器
	 */
	@Override
	public RecordReader<IntWritable, IntWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new FindMaxValueRecordReader();
	}

}
