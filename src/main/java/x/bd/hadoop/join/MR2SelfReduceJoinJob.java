package x.bd.hadoop.join;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import x.bd.hadoop.join.base.DataJoinMapperBase;
import x.bd.hadoop.join.base.DataJoinReducerBase;
import x.bd.hadoop.join.base.TaggedValue;

/**
 * 使用Hadoop API 对数据进行 Reduce 连接</br>
 * 
 * 文件1：A.txt 1,a</br> 2,b </br> 3,c
 * 
 * 文件2：B.txt 1,11</br> 1,111</br> 2,22</br> 2,222</br>4,44
 * 
 * 关联查询(要求inner join)： 1,a,11</br> 1,a,111</br> 2,b,22 </br> 2,b,222</br>
 * 
 * 
 * @author shilei
 *
 */
public class MR2SelfReduceJoinJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new MR2SelfReduceJoinJob(), args);
		if (res == 0) {
			System.out.println("MR2SelfReduceJoinJob  success !");
		} else {
			System.out.println("MR2SelfReduceJoinJob  error ! ");

		}
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String in = "/Users/shilei/Root/Develop/DevelopSpace/Test/demo/in";
		String out = "/Users/shilei/Root/Develop/DevelopSpace/Test/demo/out";

		Path inPath = new Path(in);
		Path outPath = new Path(out);

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		fs.delete(outPath, true);

		Job job = Job.getInstance(conf, "MR2SelfReduceJoinJob");

		job.setJarByClass(MR2SelfReduceJoinJob.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setNumReduceTasks(1);

		// 处理map的输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TaggedValue.class);

		job.setOutputKeyClass(Writable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}

	// Map=======================================
	/**
	 * 
	 * 1 根据文件名作为tag，区分数据来源 2 将数据封装成TaggedMapOutput 对象，并打上必要的tag 3 生成group
	 * by的分组key，作为依据
	 * 
	 * */
	public static class JoinMapper extends DataJoinMapperBase<Object, Text> {
		/**
		 * 读取输入的文件路径
		 * 
		 */
		@Override
		protected Text generateInputTagByFile(String inputFilePath, String inputFileName) {
			// 取文件名的A和B作为来源标记
			String datasource = StringUtils.splitByWholeSeparatorPreserveAllTokens(inputFileName, ".", 2)[0];
			return new Text(datasource);
		}

		/**
		 * 按需峰值要处理的记录，这里只需要原样输出
		 */
		@Override
		protected TaggedValue generateTaggedMapValue(Text value) {
			return new TaggedValue(value);
		}

		/**
		 * 数据的第一个字段作为分组key
		 */
		@Override
		protected Text generateGroupKey(TaggedValue tagValue) {
			String line = ((Text) tagValue.getData()).toString();
			if (StringUtils.isBlank(line)) {
				return null;
			}
			// 去每个文件的第一个字段作为连接key
			String groupKey = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ",", 2)[0];
			return new Text(groupKey);
		}
	}

	// Reduce============================================
	public static class JoinReducer extends DataJoinReducerBase<Writable, NullWritable> {
		private Text key = new Text("B");

		@Override
		protected void combine(SortedMap<Text, List<TaggedValue>> valueGroups, Reducer<Text, TaggedValue, Writable, NullWritable>.Context context) throws IOException, InterruptedException {
			// 必须能连接上
			if (valueGroups.size() < 2) {
				return;
			}

			// 这里只输出文件2的字段
			// 写出
			List<TaggedValue> cookieValues = valueGroups.get(key);
			Iterator<TaggedValue> iter = cookieValues.iterator();
			while (iter.hasNext()) {
				TaggedValue value = iter.next();
				if (value == null) {
					continue;
				}
				context.write(value.getData(), NullWritable.get());
			}
		}
	}
}
