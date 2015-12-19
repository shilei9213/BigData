package x.bd.hadoop.job.imputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * 保存部分数据信息
 * 
 * @author shilei
 * 
 */
public class FindMaxValueInputSplit extends InputSplit {
	// 用来计算偏移
	private int start;
	private int end;
	// 用于缓存数据
	private IntWritable[] intWritableArray;

	public FindMaxValueInputSplit(int start, int end, int[] dataAll) {
		this.start = start;
		this.end = end;
		intWritableArray = new IntWritable[dataAll.length];
		for (int i = 0, j = start; j < intWritableArray.length; j++) {
			IntWritable intWritable = new IntWritable();
			intWritable.set(dataAll[j]);
			intWritableArray[i] = intWritable;
		}
	}

	/**
	 * 获取该分片所包含的记录条数——多用比较大小和
	 */
	@Override
	public long getLength() throws IOException, InterruptedException {
		return intWritableArray.length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] { "1", "2" };
	}

	public int getStart() {
		return start;
	}

	public int getEnd() {
		return end;
	}

	public IntWritable[] getIntWritableArray() {
		return intWritableArray;
	}
	
}
