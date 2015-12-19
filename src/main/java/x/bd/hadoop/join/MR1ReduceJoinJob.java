package x.bd.hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

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
public class MR1ReduceJoinJob {
	public static void main(String[] args) throws Exception {
		String in = "/Users/shilei/Root/Develop/DevelopSpace/Test/demo/in";
		String out = "/Users/shilei/Root/Develop/DevelopSpace/Test/demo/out";

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		JobConf job = buildJob(new Path(in), new Path(out), fs, conf);
		RunningJob runningJob = JobClient.runJob(job);
		// 等待结束
		runningJob.waitForCompletion();
		if (runningJob.isSuccessful()) {
			System.out.println("success ! ");
		} else {
			System.out.println(runningJob.getFailureInfo());
		}
	}

	public static JobConf buildJob(Path in, Path out, FileSystem fs, Configuration conf) throws IOException {
		fs.delete(out, true);

		JobConf job = new JobConf(new Configuration(), MR1ReduceJoinJob.class);
		job.setJobName("MR1ReduceJoinJob");

		job.setJarByClass(MR1ReduceJoinJob.class);

		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setNumReduceTasks(1);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TaggedWritable.class);

		job.set("mapred.textoutputformat.separator", ",");

		// 解决死循环问题
		job.setLong("datajoin.maxNumOfValuesPerGroup", Long.MAX_VALUE);

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		return job;
	}

	// Map=======================================
	/**
	 * 
	 * 1 根据文件名作为tag，区分数据来源 2 将数据封装成TaggedMapOutput 对象，并打上必要的tag 3 生成group
	 * by的分组key，作为依据
	 * 
	 * */
	public static class JoinMapper extends DataJoinMapperBase {

		/**
		 * 读取输入的文件路径
		 * 
		 * **/
		protected Text generateInputTag(String inputFile) {
			// 取文件名的A和B作为来源标记
			String datasource = StringUtils.splitByWholeSeparatorPreserveAllTokens(inputFile, ".", 2)[0];
			return new Text(datasource);
		}

		/***
		 * 分组的Key
		 * 
		 * **/
		protected Text generateGroupKey(TaggedMapOutput aRecord) {
			String line = ((Text) aRecord.getData()).toString();
			if (StringUtils.isBlank(line)) {
				return null;
			}
			// 去每个文件的第一个字段作为连接key
			String groupKey = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ",", 2)[0];
			return new Text(groupKey);
		}

		/**
		 * 对文件打上标记
		 */
		protected TaggedMapOutput generateTaggedMapOutput(Object value) {
			TaggedWritable retv = new TaggedWritable((Text) value);
			retv.setTag(this.inputTag);
			return retv;
		}
	}

	// 自定义输出类型=====================================================
	public static class TaggedWritable extends TaggedMapOutput {
		private Writable data;

		// 需要定义默认构造函数，否则报错
		public TaggedWritable() {
			this.tag = new Text();
		}

		public TaggedWritable(Writable data) {
			this.tag = new Text("");
			this.data = data;
		}

		public Writable getData() {
			return data;
		}

		public void setData(Writable data) {
			this.data = data;
		}

		public void write(DataOutput out) throws IOException {
			this.tag.write(out);
			// 由于定义类型为WriteAble 所以不好使
			out.writeUTF(this.data.getClass().getName());
			this.data.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			this.tag.readFields(in);
			String dataClz = in.readUTF();
			try {
				if (this.data == null || !this.data.getClass().getName().equals(dataClz)) {
					this.data = (Writable) ReflectionUtils.newInstance(Class.forName(dataClz), null);
				}
				this.data.readFields(in);
			} catch (ClassNotFoundException cnfe) {
				System.out.println("Problem in TaggedWritable class, method readFields.");
			}
		}
	}

	// Reduce============================================
	public static class JoinReducer extends DataJoinReducerBase {

		/**
		 * tags，标签集合，且有顺序通常按照文件读取顺序 values，标签值，
		 * 
		 * 本方法被调一次，会传递一组要连接的记录，文件1的一条，文件2的一条
		 */
		protected TaggedMapOutput combine(Object[] tags, Object[] values) {
			// 按照需求，非left join 或 right join所以要求
			if (tags.length < 2)
				return null;

			String joinedStr = "";
			for (int i = 0; i < values.length; i++) {
				// 设置拼接符
				if (i > 0)
					joinedStr += ",";
				TaggedWritable tw = (TaggedWritable) values[i];
				String line = ((Text) tw.getData()).toString();
				String[] tokens = line.split(",", 2);
				joinedStr += tokens[1];
			}
			// 写出
			TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
			retv.setTag((Text) tags[0]);
			return retv;
		}
	}
}
