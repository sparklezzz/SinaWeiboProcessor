package sinaweibo.app;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
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

import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;
import sinaweibo.math.Vector.Element;

/*
 * 
 *  input text format: <weiboid>\t<content>\t<url>\t<uid>\t<timestamp>\t... 
 *   (assuming there is no other tab in the line)
 *  
 *  merge all content of one user to one line
 * 
 */

public class WeiboMergerByUser {

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		private static final Text newKey = new Text();
		private static final Text newVal = new Text();
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String []lst = value.toString().split("\t");
			if (lst.length < 5)
				return;
			
			newKey.set(lst[3]);
			newVal.set(lst[1]);
			
			context.write(newKey, newVal);
		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, Text> {

		private static final Text newVal = new Text();
		private static final StringBuffer m_buffer = new StringBuffer();
		
		private HashSet<String> m_uidSet = null;

		private static final int m_maxSizePerDoc = 256 * 1024;
		
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
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			if (m_uidSet != null && !m_uidSet.contains(key.toString()))
				return;
			
			m_buffer.delete(0, m_buffer.length());
			//m_buffer.append(GlobalName.WEIBO_CONTENT_TAG + "\t");
			
			for (Text value : values) {				
				m_buffer.append(value.toString() + " ");
				if (m_buffer.length() > m_maxSizePerDoc)
					break;
			}

			newVal.set(m_buffer.toString());
			context.write(key, newVal);
			
		}
	}
	
	private static void runMapReduceJob(Configuration conf, Path path,
			Path path2, String uidDirStr) throws IOException, InterruptedException,
			ClassNotFoundException {
		conf.set(GlobalName.UID_DIR, uidDirStr);

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);
		//job.setNumReduceTasks(1);

		job.setJarByClass(WeiboMergerByUser.class);// 主类
		job.setMapperClass(MyMapper.class);// mapper
		job.setReducerClass(MyReducer.class);// reducer

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

		if (otherArgs.length < 3) {
			System.err.println("Usage: " + className + " <uid-indir: if \"NONE\" means no uid restrict> <weibo-indir> <outdir>");
			System.exit(2);
		}

		
		runMapReduceJob(conf, new Path(otherArgs[1]), new Path(otherArgs[2]), otherArgs[0]);
	}
}
