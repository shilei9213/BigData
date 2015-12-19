package x.bd.hadoop.job.template;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

}
