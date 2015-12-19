package x.bd.hadoop.job.template;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * hadoop 启动job 模版
 * 
 * @author shilei
 * 
 */
public class JobDriverTemplate {

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			/**
			 * 使用bin/hadoop jar MyJob.jar com.xxx.MyJobDriver
			 * -Dmapred.reduce.tasks=5 启动Job时自动将 mapred.reduce.tasks 设置到conf中
			 */
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			Job job = Job.getInstance(conf, "Hadoop Client Template");
			job.setJarByClass(JobDriverTemplate.class);
			job.setMapperClass(TMapper.class);
			job.setCombinerClass(TReducer.class);
			job.setPartitionerClass(TPartitioner.class);
			job.setReducerClass(TReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
