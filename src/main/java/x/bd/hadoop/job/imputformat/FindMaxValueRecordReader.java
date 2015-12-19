package x.bd.hadoop.job.imputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 自定义RecordReader ，根据InputSplit 信息读取每一个key，value
 * 
 * 本部分定义key为偏移量，value为数据
 * 
 * @author shilei
 * 
 */
public class FindMaxValueRecordReader extends
		RecordReader<IntWritable, IntWritable> {

	// key value 缓存
	private IntWritable key;
	private IntWritable value;

	// 分片信息
	private FindMaxValueInputSplit split;
	private int cursor = 0;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.split = (FindMaxValueInputSplit) split;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (cursor < split.getLength()) {
			if (key == null) {
				key = new IntWritable();
				key.set(split.getStart() + cursor);
			}

			value = split.getIntWritableArray()[cursor];
			return true;
		}
		return false;
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public IntWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	/**
	 * 获取map执行进度
	 * 
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return Math.min(1.0f, cursor / split.getIntWritableArray().length);
	}

	@Override
	public void close() throws IOException {

	}

}
