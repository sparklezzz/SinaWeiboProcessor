package sinaweibo.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * 
 * 生成无向图（follow或者被follow，只算一条）
 * 
 */

public class GenSequenceUndirectedGraph {

	public static class MyMapper1 extends Mapper<Text, Text, Text, Text> {
		private final Text newKey = new Text();
		private final Text newVal = new Text();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			Long uid = Long.parseLong(key.toString());

			String lst[] = value.toString().split(" ");
			newVal.set(key);
			for (String s : lst) {
				if (s.isEmpty())
					continue;
				Long vid = Long.parseLong(s);

				if (uid == vid) {
					continue;
				} else if (uid < vid) {
					newKey.set(uid.toString() + "_" + vid.toString());
				} else {
					newKey.set(vid.toString() + "_" + uid.toString());
				}
				context.write(newKey, newVal);
			}
		}
	}

	public static class MyReducer1 extends Reducer<Text, Text, Text, Text> {
		private final Text newKey = new Text();
		private final Text newVal = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String lst[] = key.toString().split("_");
			if (lst.length < 2)
				return;
			String u = lst[0];
			String v = lst[1];
			boolean hasU = false, hasV = false;

			for (Text val : values) {
				if (val.toString().equals(u))
					hasU = true;
				else if (val.toString().equals(v))
					hasV = true;
			}
			if (hasU || hasV) {
				newKey.set(u);
				newVal.set(v);
				context.write(newKey, newVal);
			}
		}
	}

	public static class MyReducer2 extends Reducer<Text, Text, Text, Text> {
		private final Text newVal = new Text();
		private final StringBuffer m_buffer = new StringBuffer();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			m_buffer.delete(0, m_buffer.length());

			for (Text val : values) {
				m_buffer.append(val.toString() + " ");
			}
			newVal.set(m_buffer.toString());
			context.write(key, newVal);
		}
	}

	private static void process(Configuration conf, Path path, Path path2)
			throws IOException, InterruptedException, ClassNotFoundException {

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);

		job.setJarByClass(ProfilePropertyCooccurCalculator.class);// 主类
		job.setMapperClass(MyMapper1.class);// mapper
		job.setReducerClass(MyReducer1.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(Text.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, path);// 文件输入
		FileOutputFormat.setOutputPath(job, path2);// 文件输出
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}

	private static void process2(Configuration conf, Path path, Path path2)
			throws IOException, InterruptedException, ClassNotFoundException {

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);

		job.setJarByClass(ProfilePropertyCooccurCalculator.class);// 主类
		job.setMapperClass(Mapper.class);// mapper
		job.setReducerClass(MyReducer2.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(Text.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, path);// 文件输入
		FileOutputFormat.setOutputPath(job, path2);// 文件输出
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		if (otherArgs.length < 2) {
			System.err.println("Usage: " + className + " <indir> <outdir>");
			System.exit(2);
		}

		Path tmpPath = new Path(otherArgs[1] + "_temp");
		FileSystem.get(conf).delete(tmpPath);
		process(conf, new Path(otherArgs[0]), tmpPath);
		process2(conf, tmpPath, new Path(otherArgs[1]));
	}

}
