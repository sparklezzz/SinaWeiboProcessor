package sinaweibo.app;

/*
 * 
 * 从图中统计每种属性的一致比率
 * 其中某个属性的一致比率 = 两个端点对应用户属性值一致的边 / 两个端点对应用户属性值都合法的边
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

public class GenPropertyMatchProbFromGraph {

	public static class MyReducer1 extends Reducer<Text, Text, Text, Text> {
		private final Text newKey = new Text();
		private final Text newVal = new Text();

		private HashMap<String, String> m_userProfile1 = new HashMap<String, String>();

		private HashMap<String, String> m_userProfile2 = new HashMap<String, String>();

		boolean containsCommonVal(String v1, String v2) {
			String[] lst1 = v1.split(" ");
			String[] lst2 = v2.split(" ");
			for (String e1 : lst1) {
				for (String e2 : lst2) {
					if (e1.equals(e2))
						return true;
				}
			}
			return false;
		}

		@Override
		protected void setup(Context ctx) throws IOException,
				InterruptedException {
			super.setup(ctx);
			Configuration conf = ctx.getConfiguration();
			String ProfileChunkPathStr1 = conf
					.get(GlobalName.CURR_CHUNK_PROFILE_FILE_PATH1);
			String ProfileChunkPathStr2 = conf
					.get(GlobalName.CURR_CHUNK_PROFILE_FILE_PATH2);

			Path ProfileChunkPath1 = new Path(ProfileChunkPathStr1.trim());
			for (Pair<Text, Text> record : new SequenceFileIterable<Text, Text>(
					ProfileChunkPath1, true, conf)) {
				m_userProfile1.put(record.getFirst().toString(), record
						.getSecond().toString());
			}

			if (ProfileChunkPathStr1.equals(ProfileChunkPathStr2)) {
				m_userProfile2 = m_userProfile1;

			} else {
				Path ProfileChunkPath2 = new Path(ProfileChunkPathStr2.trim());
				for (Pair<Text, Text> record : new SequenceFileIterable<Text, Text>(
						ProfileChunkPath2, true, conf)) {
					m_userProfile2.put(record.getFirst().toString(), record
							.getSecond().toString());
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String u = key.toString();
			if (!m_userProfile1.containsKey(u))
				return;

			int validSum = 0; // 边的两端点对应用户有相同有效的属性值的边数
			int totalSum = 0; // 边的两端点对应用户都存在该属性值的边数

			for (Text value : values) {
				String[] lst = value.toString().split(" ");
				for (String v : lst) {
					if (!m_userProfile2.containsKey(v))
						continue;

					totalSum++;
					if (containsCommonVal(m_userProfile1.get(u),
							m_userProfile2.get(v))) {
						validSum++;
					}
				}
			}

			newVal.set(new Integer(validSum).toString() + "\t"
					+ new Integer(totalSum).toString());
			context.write(newKey, newVal);
		}
	}

	/*
	 * public static class MyReducer1 extends Reducer<Text, Text, Text, Text> {
	 * private final Text newKey = new Text(); private final Text newVal = new
	 * Text();
	 * 
	 * private HashMap<String, HashMap<String, HashSet<String> > > m_userProfile
	 * = new HashMap<String, HashMap<String, HashSet<String> > >(); private
	 * HashSet<String> m_propertySet = new HashSet<String>();
	 * 
	 * private HashSet<String> getValidValues(JSONObject jsonObj, String key) {
	 * HashSet<String> res = new HashSet<String>(); if (key.equals("id") ||
	 * !jsonObj.has(key)) return res; Object valObj = jsonObj.get(key); if
	 * (valObj instanceof Integer) { Integer valInt = (Integer)valObj; if
	 * (valInt != 0) res.add(valInt.toString()); } else if (valObj instanceof
	 * String) {
	 * 
	 * String []lst = ((String)valObj).split(" "); for (String s: lst) { if
	 * (!s.isEmpty() && !s.equals("0")) res.add(s); } } return res; }
	 * 
	 * private void ParseOneUserProfile(Text oneRec) {
	 * 
	 * HashMap<String, HashSet<String> > propertyMap = new HashMap<String,
	 * HashSet<String> >();
	 * 
	 * JSONObject jsonObj = new JSONObject(oneRec.toString()); if
	 * (!jsonObj.has("id")) return; String id = jsonObj.getString("id");
	 * 
	 * 
	 * Iterator<?> propertyIter = jsonObj.keys(); while ( propertyIter.hasNext()
	 * ) { String property = (String)propertyIter.next(); if (property.isEmpty()
	 * || property.equals("id") || !property.startsWith("norm_")) continue;
	 * 
	 * m_propertySet.add(property); HashSet<String> values =
	 * getValidValues(jsonObj, property); if (values.size() > 0) {
	 * propertyMap.put(property, values); } }
	 * 
	 * m_userProfile.put(id, propertyMap); }
	 * 
	 * boolean containsCommonVal(HashSet<String> hs1, HashSet<String> hs2) { for
	 * (String elem1: hs1) { if (hs2.contains(elem1)) return true; } return
	 * false; }
	 * 
	 * @Override protected void setup(Context ctx) throws IOException,
	 * InterruptedException { super.setup(ctx); Configuration conf =
	 * ctx.getConfiguration(); String curProfileChunkPathStr =
	 * conf.get(GlobalName.CURR_CHUNK_PROFILE_FILE_PATH); Path
	 * curProfileChunkPath = new Path(curProfileChunkPathStr);
	 * 
	 * for (Pair<Text, NullWritable> record : new SequenceFileIterable<Text,
	 * NullWritable>(curProfileChunkPath, true, conf)) {
	 * ParseOneUserProfile(record.getFirst()); }
	 * 
	 * //String size = SizeOf.humanReadable(SizeOf.deepSizeOf(m_userProfile));
	 * //System.out.println(size); }
	 * 
	 * 
	 * public void reduce(Text key, Iterable<Text> values, Context context)
	 * throws IOException, InterruptedException {
	 * 
	 * String u = key.toString(); if (!m_userProfile.containsKey(u)) return;
	 * HashMap<String, HashSet<String> > uMap = m_userProfile.get(u);
	 * 
	 * for (Text value : values) { String []lst = value.toString().split(" ");
	 * for (String v: lst) { if (!m_userProfile.containsKey(v)) return;
	 * HashMap<String, HashSet<String> > vMap = m_userProfile.get(v); for
	 * (String prop: m_propertySet) { newKey.set(prop); if
	 * (uMap.containsKey(prop) && vMap.containsKey(prop) &&
	 * containsCommonVal(uMap.get(prop), vMap.get(prop))) { newVal.set("1"); }
	 * else { newVal.set("-1"); } context.write(newKey, newVal); } } }
	 * 
	 * } }
	 * 
	 * public static class MyReducer2 extends Reducer<Text, Text, Text, Text> {
	 * private final Text newVal = new Text();
	 * 
	 * public void reduce(Text key, Iterable<Text> values, Context context)
	 * throws IOException, InterruptedException { int validSum = 0; int totalSum
	 * = 0;
	 * 
	 * for (Text val : values) { if (val.toString().equals("1")) validSum ++;
	 * totalSum ++; }
	 * 
	 * newVal.set(new Integer(validSum).toString() + "\t" + new
	 * Integer(totalSum).toString()); context.write(key, newVal); } }
	 */

	public static class MyMergeReducer extends
			Reducer<Text, Text, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int validSum = 0;
			int totalSum = 0;

			for (Text val : values) {
				String[] lst = val.toString().split("\t");
				validSum += Integer.parseInt(lst[0]);
				totalSum += Integer.parseInt(lst[1]);
			}

			context.write(key, new DoubleWritable(validSum / (double) totalSum));

		}
	}

	private static String getCommaSeparatedPaths(Iterable<Path> paths) {
		StringBuilder commaSeparatedPaths = new StringBuilder(100);
		String sep = "";
		for (Path path : paths) {
			commaSeparatedPaths.append(sep).append(path.toString());
			sep = ",";
		}
		return commaSeparatedPaths.toString();
	}

	private static void genPartialPropertyCount(Configuration conf, Path path,
			Path path2, String curProfileChunkPath1, String curProfileChunkPath2)
			throws IOException, InterruptedException, ClassNotFoundException {
		conf.set(GlobalName.CURR_CHUNK_PROFILE_FILE_PATH1, curProfileChunkPath1);
		conf.set(GlobalName.CURR_CHUNK_PROFILE_FILE_PATH2, curProfileChunkPath2);

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);

		job.setJarByClass(GenPropertyMatchProbFromGraph.class);// 主类
		job.setMapperClass(Mapper.class);// mapper
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

	private static void MergePartialPropertyCount(Configuration conf,
			Iterable<Path> partialPaths, Path path2) throws IOException,
			InterruptedException, ClassNotFoundException {

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		Job job = new Job(conf, className);
		job.setNumReduceTasks(1);

		job.setJarByClass(GenPropertyMatchProbFromGraph.class);// 主类
		job.setMapperClass(Mapper.class);// mapper
		job.setReducerClass(MyMergeReducer.class);// reducer

		// map 输出Key的类型
		job.setMapOutputKeyClass(Text.class);
		// map输出Value的类型
		job.setMapOutputValueClass(Text.class);
		// reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// reduce输出Value的类型
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat
				.addInputPaths(job, getCommaSeparatedPaths(partialPaths));// 文件输入
		FileOutputFormat.setOutputPath(job, path2);// 文件输出
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}

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

		if (otherArgs.length < 3) {
			System.err
					.println("Usage: "
							+ className
							+ " <profile-separate-property-chunk-indir> <graph-indir> <outdir>");
			System.exit(2);
		}

		FileSystem fs = FileSystem.get(conf);
		String profileChunkDir = otherArgs[0];
		String graphInputPathStr = otherArgs[1];
		String outDir = otherArgs[2];

		fs.mkdirs(new Path(outDir));
		// process one property each time
		for (String property : GlobalName.PROFILE_PROPERTY_SET) {
			System.out.println("Processing property " + property + " ...");

			int partialIndex = 0;
			Collection<Path> partialPaths = Lists.newArrayList();
			FileStatus[] status = fs.globStatus(new Path(profileChunkDir,
					GlobalName.PROFILE_CHUNK_FILE_PREFIX + property + "*"));
			Path[] listedPaths = FileUtil.stat2Paths(status);
			for (Path p1 : listedPaths) {
				for (Path p2 : listedPaths) {
					Path partialOutputPath = new Path(outDir, property
							+ "_stat_partial_" + (partialIndex++));
					partialPaths.add(partialOutputPath);
					genPartialPropertyCount(conf, new Path(graphInputPathStr),
							partialOutputPath, p1.toString(), p2.toString());
				}
			}

			MergePartialPropertyCount(conf, partialPaths, new Path(outDir,
					property + "_stat"));
			for (Path partial : partialPaths) {
				// fs.delete(partial);
			}
		}
	}

}
