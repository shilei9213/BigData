package x.bd.hadoop.job.imputformat;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindMaxValueDriver {

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf, "Inputformat Test");
			job.setJarByClass(FindMaxValueDriver.class);
			job.setInputFormatClass(FindMaxValueInputFormat.class);
			job.setMapperClass(FindMaxValueMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
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

	/**
	 * 汇总入队列
	 * 
	 * @author shilei
	 * 
	 */
	static class FindMaxValueMapper extends
			Mapper<IntWritable, IntWritable, Text, IntWritable> {

		@Override
		protected void map(IntWritable key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			System.out.println("num: " + value);
			context.write(new Text("queue"), value);
		}
	}

	/**
	 * 求最大值
	 * 
	 * @author shilei
	 * 
	 */
	static class FindMaxValueReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int max = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				int next = iterator.next().get();
				if (next > max) {
					max = next;
				}
			}
			context.write(new Text("Max"), new IntWritable(max));
		}

	}

}
