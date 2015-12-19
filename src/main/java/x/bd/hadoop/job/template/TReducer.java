package x.bd.hadoop.job.template;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		super.reduce(key, values, context);
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
