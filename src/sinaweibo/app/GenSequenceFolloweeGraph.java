package sinaweibo.app;

/*
 * 
 * 生成单向follow的图（我关注的人）
 * 
 */

import java.io.IOException;
import java.util.HashSet;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONException;

import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;

public class GenSequenceFolloweeGraph {

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		private final Text newKey = new Text();
		private final Text newVal = new Text();
		private final StringBuffer m_buffer = new StringBuffer();
		private HashSet<String> m_uidSet = null;

		@Override
		protected void setup(Context ctx) throws IOException,
				InterruptedException {
			super.setup(ctx);
			Configuration conf = ctx.getConfiguration();
			String uidDirStr = conf.get(GlobalName.UID_DIR, "NONE");
			if (uidDirStr.toLowerCase().equals("none")) { // no need to filter
															// by users
				return;
			}
			m_uidSet = new HashSet<String>();

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.globStatus(new Path(uidDirStr, "*"));
			Path[] listedPaths = FileUtil.stat2Paths(status);
			for (Path uidFilePath : listedPaths) {
				if (uidFilePath.getName().toString().startsWith("_"))
					continue;
				for (Pair<Text, NullWritable> record : new SequenceFileIterable<Text, NullWritable>(
						uidFilePath, true, conf)) {
					m_uidSet.add(record.getFirst().toString().split("\t")[0]);
				}
			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			try {
				JSONArray jsonArr = new JSONArray(value.toString());
				Long uid = Long.MAX_VALUE;
				if (jsonArr.get(0) instanceof Integer
						|| jsonArr.get(0) instanceof Long) {
					uid = jsonArr.getLong(0);
				} else if (jsonArr.get(0) instanceof String) {
					uid = Long.parseLong(jsonArr.getString(0));
				}
				if (uid == Long.MAX_VALUE)
					return;
				if (!(jsonArr.get(1) instanceof JSONArray))
					return;

				if (m_uidSet != null && !m_uidSet.contains(uid.toString()))
					return;

				newKey.set(uid.toString());
				m_buffer.delete(0, m_buffer.length());
				JSONArray subJsonArr = jsonArr.getJSONArray(1);
				for (int j = 0; j < subJsonArr.length(); ++j) {
					Long vid = Long.MAX_VALUE;
					if (subJsonArr.get(j) instanceof Integer
							|| subJsonArr.get(j) instanceof Long) {
						vid = subJsonArr.getLong(j);
					} else if (subJsonArr.get(j) instanceof String) {
						vid = Long.parseLong(subJsonArr.getString(j));
					}
					if (vid == Long.MAX_VALUE)
						continue;

					if (m_uidSet != null && !m_uidSet.contains(vid.toString()))
						continue;
					m_buffer.append(vid.toString() + " ");
				}
				newVal.set(m_buffer.toString());
				context.write(newKey, newVal);
			} catch (JSONException e) {
				System.out
						.println("Fail to decode json str" + value.toString());
			}
		}
	}

	private static void runMapReduceJob(Configuration conf, Path path, Path path2,
			String uidDirStr) throws IOException, InterruptedException,
			ClassNotFoundException {
		conf.set(GlobalName.UID_DIR, uidDirStr);

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);

		job.setJarByClass(ProfilePropertyCooccurCalculator.class);// 主类
		job.setMapperClass(MyMapper.class);// mapper
		job.setReducerClass(Reducer.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(Text.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
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

		if (otherArgs.length < 3) {
			System.err
					.println("Usage: "
							+ className
							+ " <uid-indir: if \"NONE\" means no uid restrict> <graph-indir> <outdir>");
			System.exit(2);
		}

		runMapReduceJob(conf, new Path(otherArgs[1]), new Path(otherArgs[2]),
				otherArgs[0]);

	}

}
