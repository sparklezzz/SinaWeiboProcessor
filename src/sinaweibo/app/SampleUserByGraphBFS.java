package sinaweibo.app;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.Lists;

import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;

/*
 * 
 *  input text format: sequence file of uid link graph
 *  
 *  try to sample user using several times BFS
 * 
 */

public class SampleUserByGraphBFS {

	private static final String SEED_USER_RATIO = "SEED_USER_RATIO";
	private static final String IS_FIRST_BFS = "IS_FIRST_BFS";
	private static final String UID_FILE_PATH_STR = "UID_FILE_PATH_STR";

	public static class MyMapper extends Mapper<Text, Text, Text, NullWritable> {
		private static final Text newKey = new Text();
		// private static final Text newVal = new Text();
		private static float m_ratio = (float) 0.0;
		private static boolean m_isFirstBFS = true;
		private static HashSet<String> m_uidSet = null;

		@Override
		protected void setup(Context ctx) throws IOException,
				InterruptedException {
			super.setup(ctx);
			Configuration conf = ctx.getConfiguration();
			m_isFirstBFS = conf.getBoolean(IS_FIRST_BFS, true);
			if (m_isFirstBFS) {
				m_ratio = conf.getFloat(SEED_USER_RATIO, (float) 0.0);
			} else {
				m_uidSet = new HashSet<String>();
				String uidFilePathStrLst = conf.get(UID_FILE_PATH_STR);
				for (String uidFilePathStr : uidFilePathStrLst.split(",")) {
					Path uidFilePath = new Path(uidFilePathStr.trim());
					if (uidFilePath.getName().toString().startsWith("_"))
						continue;
					for (Pair<Text, NullWritable> record : new SequenceFileIterable<Text, NullWritable>(
							uidFilePath, true, conf)) {
						m_uidSet.add(record.getFirst().toString().split("\t")[0]);
					}
				}
			}
		}

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			if (m_isFirstBFS) {
				if (Math.random() >= m_ratio)
					return;
			} else {
				if (!m_uidSet.contains(key.toString()))
					return;
			}

			context.write(key, NullWritable.get());
			String[] lst = value.toString().split(" ");
			for (String v : lst) {
				newKey.set(v);
				context.write(newKey, NullWritable.get());
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		// private static final Text newVal = new Text();
		// private static final StringBuffer m_buffer = new StringBuffer();

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {

			context.write(key, NullWritable.get());
			context.getCounter("MyCounter", "TotalUserSampled").increment(1);
		}
	}

	private static String getCommaSeparatedPaths(Path[] paths) {
		StringBuilder commaSeparatedPaths = new StringBuilder(100);
		String sep = "";
		for (Path path : paths) {
			commaSeparatedPaths.append(sep).append(path.toString());
			sep = ",";
		}
		return commaSeparatedPaths.toString();
	}

	private static void runMapReduceJob(Configuration conf, Path path,
			Path path2, float firstRoundRatio, int BFS_time, int max_BFS_time)
			throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		Path inPath, outPath;
		inPath = path;
		if (BFS_time == 1) {
			conf.setBoolean(IS_FIRST_BFS, true);
			conf.setFloat(SEED_USER_RATIO, firstRoundRatio);
		} else {
			conf.setBoolean(IS_FIRST_BFS, false);
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.globStatus(new Path(path2.toString()
					+ "/BFS_" + (BFS_time - 1), "part*"));
			Path[] listedPaths = FileUtil.stat2Paths(status);
			conf.set(UID_FILE_PATH_STR, getCommaSeparatedPaths(listedPaths));
		}
		outPath = new Path(path2.toString() + "/BFS_" + BFS_time);

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);
		job.setNumReduceTasks(1);

		job.setJarByClass(SampleUserByGraphBFS.class);// 主类
		job.setMapperClass(MyMapper.class);// mapper
		job.setReducerClass(MyReducer.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(NullWritable.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		/*
		if (BFS_time < max_BFS_time) {
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		} else { // text output format for last round
			job.setOutputFormatClass(TextOutputFormat.class);
		}
		*/

		FileInputFormat.addInputPath(job, inPath);// 文件输入
		FileOutputFormat.setOutputPath(job, outPath);// 文件输出
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
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		if (otherArgs.length < 4) {
			System.err
					.println("Usage: "
							+ className
							+ " <First-round-sample-ratio> <BFS-depths-from-1> <indir> <outdir>");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		fs.mkdirs(new Path(otherArgs[3]));
		float firstRoundRatio = Float.parseFloat(otherArgs[0]);
		int maxDepth = Integer.parseInt(otherArgs[1]);
		for (int i = 1; i <= maxDepth; i++) {
			runMapReduceJob(conf, new Path(otherArgs[2]),
					new Path(otherArgs[3]), firstRoundRatio, i, maxDepth);
		}
	}
}
