package sinaweibo.app;

/*
 * 
 * 统计两个profile属性值之间的共现比率
 * 统计方法：属性A,B 值va, vb
 * prob(A_va_B_vb) = count(A_va_B_vb) / count(A_B)
 * 
 * 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONObject;

public class ProfilePropertyCooccurCalculator {

	private static final String TOTAL_USER_COUNT = "TOTAL_USER_COUNT";

	public static class MyMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final Text newKey = new Text();
		private Map<String, ArrayList<String>> m_propertyMap = new HashMap<String, ArrayList<String>>();

		ArrayList<String> getValidValues(JSONObject jsonObj, String key) {
			ArrayList<String> res = new ArrayList<String>();
			if (key.equals("id") || !jsonObj.has(key))
				return res;
			Object valObj = jsonObj.get(key);
			if (valObj instanceof Integer) {
				Integer valInt = (Integer) valObj;
				if (valInt != 0)
					res.add(valInt.toString());
			} else if (valObj instanceof String) {

				String[] lst = ((String) valObj).split(" ");
				for (String s : lst) {
					if (!s.isEmpty() && !s.equals("0"))
						res.add(s);
				}
			}
			return res;
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			JSONObject jsonObj = new JSONObject(value.toString());
			if (!jsonObj.has("id"))
				return;
			String id = jsonObj.getString("id");

			// newVal.set(id);
			m_propertyMap.clear();

			Iterator<?> propertyIter = jsonObj.keys();
			while (propertyIter.hasNext()) {
				String property = (String) propertyIter.next();
				ArrayList<String> values = getValidValues(jsonObj, property);
				if (values.size() > 0) {
					m_propertyMap.put(property, values);
				}
			}

			for (Entry<String, ArrayList<String>> entry1 : m_propertyMap
					.entrySet()) {
				for (Entry<String, ArrayList<String>> entry2 : m_propertyMap
						.entrySet()) {
					if (entry1.getKey().compareTo(entry2.getKey()) >= 0)
						continue;
					for (String value1 : entry1.getValue()) {
						for (String value2 : entry2.getValue()) {
							newKey.set(entry1.getKey() + "/" + value1 + "/"
									+ entry2.getKey() + "/" + value2);
							context.write(newKey, new IntWritable(1));
						}
					}
				}
			}

		}
	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private static int totalUserCount = 0;

		@Override
		protected void setup(Context ctx) throws IOException,
				InterruptedException {
			super.setup(ctx);
			Configuration conf = ctx.getConfiguration();
			totalUserCount = conf.getInt(TOTAL_USER_COUNT, 0);
		}

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}

			context.write(key, new IntWritable(sum));

		}
	}

	public static class MyMapper2 extends Mapper<Text, IntWritable, Text, Text> {
		private final Text newKey = new Text();
		private final Text newVal = new Text();

		public void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			String[] lst = key.toString().split("/");
			if (lst.length < 4)
				return;
			newKey.set(lst[0] + "/" + lst[2]);
			newVal.set(key.toString() + "\t"
					+ new Integer(value.get()).toString());
			context.write(newKey, newVal);
		}
	}

	public static class MyReducer2 extends
			Reducer<Text, Text, Text, DoubleWritable> {
		private final Text newKey = new Text();
		private ArrayList<String> m_keyArr = new ArrayList<String>();
		private ArrayList<Integer> m_valArr = new ArrayList<Integer>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			m_keyArr.clear();
			m_valArr.clear();
			for (Text val : values) {
				String[] lst = val.toString().split("\t");
				int tmp = Integer.parseInt(lst[1]);
				sum += tmp;
				m_keyArr.add(lst[0]);
				m_valArr.add(tmp);
			}

			for (int i = 0; i < m_keyArr.size(); ++i) {
				newKey.set(m_keyArr.get(i));
				context.write(newKey, new DoubleWritable(m_valArr.get(i)
						/ (double) sum));
			}
		}
	}

	public static class MyMapper3 extends
			Mapper<Text, DoubleWritable, DoubleWritable, Text> {
		public void map(Text key, DoubleWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(value, key);
		}
	}

	public static class MyReducer3 extends
			Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}
		}
	}

	public static class ReverseComparator extends WritableComparator {

		protected ReverseComparator() {
			super(DoubleWritable.class, true);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return -1 * ((DoubleWritable) w1).compareTo((DoubleWritable) w2);
		}

	}

	/*
	 * 
	 */
	private static void process(Configuration conf, Path path, Path path2)
			throws IOException, InterruptedException, ClassNotFoundException {
		// conf.setInt(TOTAL_USER_COUNT, 0);

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);
		job.setNumReduceTasks(1);

		job.setJarByClass(ProfilePropertyCooccurCalculator.class);// 主类
		job.setMapperClass(MyMapper.class);// mapper
		job.setReducerClass(MyReducer.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(IntWritable.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, path);// 文件输入
		FileOutputFormat.setOutputPath(job, path2);// 文件输出
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}

	// 将各个分数normalize到[0，1]
	private static void process2(Configuration conf, Path path, Path path2)
			throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);
		job.setNumReduceTasks(1);

		job.setJarByClass(ProfilePropertyCooccurCalculator.class);// 主类
		job.setMapperClass(MyMapper2.class);// mapper
		job.setReducerClass(MyReducer2.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(Text.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, path);// 文件输入
		FileOutputFormat.setOutputPath(job, path2);// 文件输出
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}

	// 将各个比率按照prob降序排列
	private static void process3(Configuration conf, Path path, Path path2)
			throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);
		job.setNumReduceTasks(1);

		job.setJarByClass(ProfilePropertyCooccurCalculator.class);// 主类
		job.setMapperClass(MyMapper3.class);// mapper
		job.setReducerClass(MyReducer3.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(DoubleWritable.class);
		// map输出Value的类型
		job.setMapOutputValueClass(Text.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(DoubleWritable.class);
		job.setSortComparatorClass(ReverseComparator.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
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
	@SuppressWarnings("deprecation")
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
			System.err.println("Usage: " + className + " <indir> <outdir>");
			System.exit(2);
		}

		Path tmp1 = new Path(otherArgs[1] + "_temp1");
		Path tmp2 = new Path(otherArgs[1] + "_temp2");
		FileSystem.get(conf).delete(tmp1);
		FileSystem.get(conf).delete(tmp2);
		process(conf, new Path(otherArgs[0]), tmp1);
		process2(conf, tmp1, tmp2);
		process3(conf, tmp2, new Path(otherArgs[1]));
	}

}
