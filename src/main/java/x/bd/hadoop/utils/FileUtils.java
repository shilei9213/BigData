package x.bd.hadoop.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FileUtils {

	/**
	 * 添加输入输出目录
	 * 
	 * @param job
	 * @param fs
	 * @param path
	 * @throws IOException
	 */
	public static void addFiles(Job job, FileSystem fs, Path path) throws IOException {
		if (fs.isDirectory(path)) {
			FileStatus[] listStatus = fs.listStatus(path);
			for (FileStatus fileStatus : listStatus) {
				if (fileStatus.isDirectory()) {
					System.out.println(job.getJobName() + " : " + fileStatus.getPath().toString());
					FileInputFormat.addInputPath(job, fileStatus.getPath());
				}
			}
		}
	}

	/**
	 * 逐行读取文件
	 * 
	 * @param conf
	 * @param filePath
	 * @return
	 */
	public static Set<String> readFile(Configuration conf, String filePath) {
		Set<String> records = new HashSet<String>();

		// 遍历目录下的所有文件
		BufferedReader br = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(filePath));
			for (FileStatus file : status) {
				if (!file.getPath().getName().startsWith("part-")) {
					continue;
				}

				FSDataInputStream inputStream = fs.open(file.getPath());
				br = new BufferedReader(new InputStreamReader(inputStream));

				String line = null;
				while (null != (line = br.readLine())) {
					records.add(StringUtils.trim(line));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return records;
	}

	/**
	 * 写文件
	 * 
	 * @param conf
	 * @param filePath
	 * @param content
	 */
	public static void writeFile(Configuration conf, Path filePath, String content) {
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream out = fs.create(filePath);

			out.write(content.getBytes());

			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
