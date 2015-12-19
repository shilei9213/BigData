package x.bd.hadoop.join.base;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 连接查询Reduce
 * 
 * @author shilei
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class DataJoinReducerBase<KEYOUT, VALUEOUT> extends Reducer<Text, TaggedValue, KEYOUT, VALUEOUT> {

	@Override
	protected void setup(Reducer<Text, TaggedValue, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	/**
	 * 合并结果
	 */
	protected abstract void combine(SortedMap<Text, List<TaggedValue>> valueGroups, Reducer<Text, TaggedValue, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException;

	@Override
	protected void reduce(Text key, Iterable<TaggedValue> values, Reducer<Text, TaggedValue, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
		// 获得一个sort map ，key 是tab ，value 是结果的集合
		SortedMap<Text, List<TaggedValue>> groups = regroup(key, values, context);
		combine(groups, context);
		context.getCounter("DataJoinReucer", "groupCount").increment(1);
	}

	/**
	 * 按照Tag 对value 进行充分组
	 * 
	 * @param key
	 * @param arg1
	 * @param reporter
	 * @return
	 * @throws IOException
	 */
	private SortedMap<Text, List<TaggedValue>> regroup(Text key, Iterable<TaggedValue> values, Reducer<Text, TaggedValue, KEYOUT, VALUEOUT>.Context context) throws IOException {
		/*
		 * key： tag; value ： TaggedValue
		 */
		SortedMap<Text, List<TaggedValue>> valueGroup = new TreeMap<Text, List<TaggedValue>>();

		// 遍历Value
		Iterator<TaggedValue> iter = values.iterator();
		while (iter.hasNext()) {

			// TODO 为什么需要克隆？
			TaggedValue taggedValue = ((TaggedValue) iter.next()).clone(context.getConfiguration());
			// 获得记录的 tag
			Text tag = taggedValue.getTag();
			// 从map 中获取一个iterator，如果已经创建，就做一个情况

			List<TaggedValue> datas = valueGroup.get(tag);
			if (datas == null) {
				datas = new LinkedList<TaggedValue>();
				valueGroup.put(tag, datas);
			}
			datas.add(taggedValue);

			// System.out.println("reduce : " + taggedValue + "|" +
			// tag.toString() + "|" + taggedValue.getData().toString());
			taggedValue = null;
		}
		return valueGroup;
	}

}
