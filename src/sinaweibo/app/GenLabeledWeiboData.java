package sinaweibo.app;

/*
 * 
 * 生成带分类的微博数据
 * 
 */

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

//import net.sourceforge.sizeof.SizeOf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONObject;

import com.google.common.collect.Lists;

import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;

public class GenLabeledWeiboData {

	private static final String PROP_FILE_LIST = "PROP_FILE_LIST";
	
	public static class MyMapper extends Mapper<Text, Text, Text, Text> {
		private static final Text newKey = new Text();
		private static final Text newVal = new Text();
		
		private HashMap<String, String> m_userProfile = new HashMap<String, String>();

		@Override
		protected void setup(Context ctx) throws IOException,
				InterruptedException {
			super.setup(ctx);
			Configuration conf = ctx.getConfiguration();
			String propFileListStr = conf.get(PROP_FILE_LIST);
			for (String propFilePathStr: propFileListStr.split(",")) {
				Path propFilePath = new Path(propFilePathStr.trim());
				for (Pair<Text, Text> record : new SequenceFileIterable<Text, Text>(
						propFilePath, true, conf)) {
					m_userProfile.put(record.getFirst().toString(), record.getSecond().toString());
				}
			}
		}
		
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			//String []lst = value.toString().split("\t");
			//skip lst[0], which is content tag
			//newVal.set(lst[1]);
			
			newVal.set(value.toString());
			
			if (!m_userProfile.containsKey(key.toString())) {
				newKey.set("/-/" + key);
				context.getCounter("MyCounter", "UnlabeledUserCount").increment(1);
			} else {
				newKey.set("/" + m_userProfile.get(key) + "/" + key);
				context.getCounter("MyCounter", "LabeledUserCount").increment(1);
			}
			context.write(newKey, newVal);
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
			Path path2, String propFileListStr)
			throws IOException, InterruptedException, ClassNotFoundException {
		conf.set(PROP_FILE_LIST, propFileListStr);

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);

		job.setJarByClass(GenLabeledWeiboData.class);// 主类
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

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, path);// 文件输入
		FileOutputFormat.setOutputPath(job, path2);// 文件输出
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}

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
			System.err
					.println("Usage: "
							+ className
							+ " <profile-separate-property-chunk-indir> <merge-weibo-indir> <outdir>");
			System.exit(2);
		}

		FileSystem fs = FileSystem.get(conf);
		String profileChunkDir = otherArgs[0];
		String weiboInputPathStr = otherArgs[1];
		String outDir = otherArgs[2];

		fs.mkdirs(new Path(outDir));
		// process one property each time
		for (String property : GlobalName.PROFILE_PROPERTY_SET) {
			System.out.println("Processing property " + property + " ...");

			FileStatus[] status = fs.globStatus(new Path(profileChunkDir,
					GlobalName.PROFILE_CHUNK_FILE_PREFIX + property + "*"));
			Path[] listedPaths = FileUtil.stat2Paths(status);
			String propFileListStr = getCommaSeparatedPaths(listedPaths);
			runMapReduceJob(conf, new Path(weiboInputPathStr),
					new Path(outDir, property), propFileListStr);
		}
	}

}
