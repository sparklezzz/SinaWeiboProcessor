package sinaweibo.app;

/*
 * 
 * 生成带分类的微博数据
 * 对于标签和大学这种一个实例中有多个值的属性，随机取得其中一个
 * 每个类均衡采样
 * 
 */

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

//import net.sourceforge.sizeof.SizeOf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import com.google.common.collect.Maps;

import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;
import sinaweibo.math.VectorWritable;
import sinaweibo.vectorizer.DictionaryVectorizer;
import sinaweibo.vectorizer.tfidf.TFIDFConverter;

public class AddLabelToWeiboVector {

	private static final String PROP_FILE_LIST = "PROP_FILE_LIST";
	private static final String LABEL_COUNT_FILE = "LABEL_COUNT_FILE";
	
	public static class MyMapper1 extends Mapper<Text, VectorWritable, Text, IntWritable> {
		private static final Text newKey = new Text();
		
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
		public void map(Text key, VectorWritable value, Context context)
				throws IOException, InterruptedException {

			//String []lst = value.toString().split("\t");
			//skip lst[0], which is content tag
			//newVal.set(lst[1]);
			
			String keyStr = key.toString();
			
			if (!m_userProfile.containsKey(keyStr)) {
				return;
			} else {
				String k = m_userProfile.get(keyStr);
				if (k == null || k.equals("") || k.equals("0") || k.equals("null")) {
					return;
				} else {		
					//TODO: for multi-value index, current solution is to randomly choose one value
					String []tmpLst = k.split(" ");
					int ranIdx = (int) (Math.random() * tmpLst.length);
					
					newKey.set(tmpLst[ranIdx]);
					context.write(newKey, new IntWritable(1));
				}
			}
		}
	}
	
	public static class MyReducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class MyMapper2 extends Mapper<Text, VectorWritable, Text, VectorWritable> {
		private static final Text newKey = new Text();
		
		private HashMap<String, String> m_userProfile = new HashMap<String, String>();
		private HashMap<String, Double> m_ratioMap = null;
		
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
			
			String labelCountPathStr = conf.get(LABEL_COUNT_FILE);
			if (labelCountPathStr.toLowerCase().equals("none"))
				return;
			
			m_ratioMap = Maps.newHashMap();
			int minCount = Integer.MAX_VALUE;
			for (Pair<Text, IntWritable> record : new SequenceFileIterable<Text, IntWritable>(
					new Path(labelCountPathStr), true, conf)) {
				int c = record.getSecond().get();
				m_ratioMap.put(record.getFirst().toString(), new Double(c));
				if (minCount > c)
					minCount = c;
			}
			
			if (minCount > 0 && minCount != Integer.MAX_VALUE) {
				for (Entry<String, Double> entry: m_ratioMap.entrySet()) {
					m_ratioMap.put(entry.getKey(), minCount / (double) entry.getValue());
				}
			} else {
				System.out.println("Zero min count!");
			}
		}
		
		@Override
		public void map(Text key, VectorWritable value, Context context)
				throws IOException, InterruptedException {

			//String []lst = value.toString().split("\t");
			//skip lst[0], which is content tag
			//newVal.set(lst[1]);
			
			String keyStr = key.toString();
			
			if (!m_userProfile.containsKey(keyStr)) {
				newKey.set("/-/" + keyStr);
				context.getCounter("MyCounter", "UnlabeledUserCount").increment(1);
				return;
			} else {
				String k = m_userProfile.get(keyStr);
				if (k == null || k.equals("") || k.equals("0") || k.equals("null")) {
					newKey.set("/-/" + keyStr);
					context.getCounter("MyCounter", "UnlabeledUserCount").increment(1);
					return;
				} else {		
					//TODO: for multi-value index, current solution is to randomly choose one value
					String []tmpLst = k.split(" ");
					int ranIdx = (int) (Math.random() * tmpLst.length);
					String label = tmpLst[ranIdx];
					
					//for balanced class sampling, skip some labeled data
					if (m_ratioMap != null && m_ratioMap.containsKey(label) 
							&& Math.random() > m_ratioMap.get(label)) {
						return;
					}
										
					newKey.set("/" + label + "/" + keyStr);
					context.getCounter("MyCounter", "LabeledUserCount").increment(1);
					context.write(newKey, value);
				}
			}
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

	private static void runMapReduceJob1(Configuration conf, Path path,
			Path path2, String propFileListStr)
			throws IOException, InterruptedException, ClassNotFoundException {
		System.out.println("Writing to path " + path2.toString() + " ...");
		conf.set(PROP_FILE_LIST, propFileListStr);
		
		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);

		job.setNumReduceTasks(1);
		
		job.setJarByClass(AddLabelToWeiboVector.class);// 主类
		job.setMapperClass(MyMapper1.class);// mapper
		job.setReducerClass(MyReducer1.class);// reducer
		job.setCombinerClass(MyReducer1.class);

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(IntWritable.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, path);// 文件输入
		FileOutputFormat.setOutputPath(job, path2);// 文件输出
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}
	
	
	private static void runMapReduceJob2(Configuration conf, Path path,
			Path path2, String propFileListStr, String labelCountPathStr)
			throws IOException, InterruptedException, ClassNotFoundException {
		System.out.println("Writing to path " + path2.toString() + " ...");
		conf.set(PROP_FILE_LIST, propFileListStr);
		conf.set(LABEL_COUNT_FILE, labelCountPathStr);
		
		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);

		job.setJarByClass(AddLabelToWeiboVector.class);// 主类
		job.setMapperClass(MyMapper2.class);// mapper
		job.setReducerClass(Reducer.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(VectorWritable.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(VectorWritable.class);

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

		if (otherArgs.length < 2) {
			System.err
					.println("Usage: "
							+ className
							+ " <profile-separate-property-chunk-indir> <weibo-vector-parent-indir>");
			System.exit(2);
		}

		FileSystem fs = FileSystem.get(conf);
		String profileChunkDir = otherArgs[0];
		String weiboInputPathStr = otherArgs[1];
		//String outDir = otherArgs[2];

		//fs.mkdirs(new Path(outDir));
		// process one property each time
		for (String property : GlobalName.PROFILE_PROPERTY_SET) {
			System.out.println("Processing property " + property + " ...");

			FileStatus[] status = fs.globStatus(new Path(profileChunkDir,
					GlobalName.PROFILE_CHUNK_FILE_PREFIX + property + "*"));
			Path[] listedPaths = FileUtil.stat2Paths(status);
			String propFileListStr = getCommaSeparatedPaths(listedPaths);
			
			/*
			Path labelCountDirPath = new Path(weiboInputPathStr, 
					GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_label_count");

			runMapReduceJob1(conf, new Path(weiboInputPathStr, TFIDFConverter.DOCUMENT_VECTOR_OUTPUT_FOLDER),
					labelCountDirPath, propFileListStr);
			
			runMapReduceJob2(conf, new Path(weiboInputPathStr, TFIDFConverter.DOCUMENT_VECTOR_OUTPUT_FOLDER),
					new Path(weiboInputPathStr, GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property), 
					propFileListStr, labelCountDirPath.toString() + "/part-r-00000");
			fs.delete(labelCountDirPath);
			*/
			runMapReduceJob2(conf, new Path(weiboInputPathStr, TFIDFConverter.DOCUMENT_VECTOR_OUTPUT_FOLDER),
					new Path(weiboInputPathStr, GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property), 
					propFileListStr, "none");
		}
	}

}
