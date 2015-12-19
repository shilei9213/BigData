package x.bd.hadoop.utils;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

public class JobUtils {

	/**
	 * 执行 Job 连
	 * 
	 * @param jobs
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static String handleJobChain(Job... jobs) throws IOException, ClassNotFoundException, InterruptedException {
		StringBuilder result = new StringBuilder();
		for (Job j : jobs) {
			if (!j.waitForCompletion(true)) {
				result.append(j.getJobID().toString()).append(" fail! ").append('\n');
				return result.toString();
			}
			result.append(j.getJobID().toString()).append('\n');
			result.append(j.getCounters().toString()).append('\n');
		}

		return result.toString();
	}
}
