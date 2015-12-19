package x.bd.hadoop.job.template;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * bin/hadoop jar MyJob.jar com.xxx.MyJobDriver -Dmapred.reduce.tasks=5 \ -files
 * ./dict.conf \ -libjars
 * lib/commons-beanutils-1.8.3.jar,lib/commons-digester-2.1.jar
 * 
 * @author shilei
 * 
 */
public class JobToolRunnerTemplate extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Hadoop Client Template");
		job.setJarByClass(JobDriverTemplate.class);
		job.setMapperClass(TMapper.class);
		job.setCombinerClass(TReducer.class);
		job.setPartitionerClass(TPartitioner.class);
		job.setReducerClass(TReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		/**
		 * 使用bin/hadoop jar MyJob.jar com.xxx.MyJobDriver
		 * -Dmapred.reduce.tasks=5 启动ToolRunner.run自动将 mapred.reduce.tasks
		 * 设置到conf中，run方法通过getConf()获得数据
		 */
		int res = ToolRunner.run(new Configuration(), new JobToolRunnerTemplate(), args);
		System.exit(res);
	}

}
