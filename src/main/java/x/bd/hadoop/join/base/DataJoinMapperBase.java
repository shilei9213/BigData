package x.bd.hadoop.join.base;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 连接查询 Mapper
 * 
 * @author shilei
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public abstract class DataJoinMapperBase<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, Text, TaggedValue> {
	// 类型标记
	protected Text inputTag;
	// 输入文件路径，文件名
	protected String inputFilePath, inputFileName;

	/**
	 * 根据数据的文件名确定输入标签
	 * 
	 */
	protected abstract Text generateInputTagByFile(String inputFilePath, String inputFileName);

	/**
	 * 根据行内容处理Tag
	 * 
	 * @param inputFilePath
	 * @param inputFileName
	 * @return
	 */
	protected Text generateInputTagByLine(Text tag, KEYIN key, VALUEIN value, Context context) {
		return inputTag;
	}

	/**
	 * 封装待标签的输出
	 */
	protected abstract TaggedValue generateTaggedMapValue(VALUEIN value);

	/**
	 * 生成group by的列
	 */
	protected abstract Text generateGroupKey(TaggedValue tagValue);

	@Override
	protected void setup(Mapper<KEYIN, VALUEIN, Text, TaggedValue>.Context context) throws IOException, InterruptedException {
		FileSplit inputSplit = (FileSplit)context.getInputSplit();
		
		this.inputFilePath = inputSplit.getPath().getParent().getName();
		this.inputFileName = inputSplit.getPath().getName();
		this.inputTag = generateInputTagByFile(this.inputFilePath, this.inputFileName);
	}

	@Override
	public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		// 根据本行情况，处理Tag
		this.inputTag = generateInputTagByLine(this.inputTag, key, value, context);
		
		// 生成带标签的value
		TaggedValue taggedValue = generateTaggedMapValue(value);
		if (taggedValue == null) {
			context.getCounter("DataJoinMapper", "discardedCount").increment(1);
			return;
		}

		// 生成分组健
		Text groupKey = generateGroupKey(taggedValue);
		if (groupKey == null) {
			context.getCounter("DataJoinMapper", "nullGroupKeyCount").increment(1);
			return;
		}

		// 输出内容绑定标签
		taggedValue.setTag(this.inputTag);
		// key : group key , value : taggedValue
		context.write(groupKey, taggedValue);
		context.getCounter("DataJoinMapper", "outCount").increment(1);
	}
}
