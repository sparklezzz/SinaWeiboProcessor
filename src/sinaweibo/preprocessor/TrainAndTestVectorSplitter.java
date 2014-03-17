package sinaweibo.preprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import sinaweibo.common.AbstractJob;
import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.PathFilters;
import sinaweibo.common.iterator.sequencefile.PathType;
import sinaweibo.common.iterator.sequencefile.SequenceFileDirIterator;
import sinaweibo.math.VectorWritable;

public class TrainAndTestVectorSplitter extends AbstractJob {

	private static final String TRAINING_TAG = "training";
	private static final String TEST_TAG = "test";

	public static class MyReducer extends
			Reducer<Text, VectorWritable, Text, VectorWritable> {

		private MultipleOutputs multipleOutputs;
		private OutputCollector<Text, VectorWritable> trainingCollector = null;
		private OutputCollector<Text, VectorWritable> testCollector = null;

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException {
			multipleOutputs = new MultipleOutputs(new JobConf(
					context.getConfiguration()));
			trainingCollector = multipleOutputs
					.getCollector(TRAINING_TAG, null);
			testCollector = multipleOutputs.getCollector(TEST_TAG, null);
		}

		/**
		 * Randomly allocate key value pairs between test and training sets.
		 * randomSelectionPercent of the pairs will go to the test set.
		 */
		@Override
		protected void reduce(Text key, Iterable<VectorWritable> values,
				Context context) throws IOException, InterruptedException {

			for (VectorWritable value : values) {
				String keyStr = key.toString();
				String[] lst = keyStr.split("/");
				if (lst.length > 1) {
					if (lst[1].equals("-")) {
						testCollector.collect(key, value);
					} else {
						trainingCollector.collect(key, value);
					}
				}
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException {
			multipleOutputs.close();
		}

	}

	@SuppressWarnings("rawtypes")
	@Override
	public int run(String[] args) throws Exception {
		// public static void process(Configuration initialConf, Path inputPath,
		// Path outputPath) throws IOException, InterruptedException,
		// ClassNotFoundException {
		// Determine class of keys and values

		Configuration initialConf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(initialConf, args)
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

		FileSystem fs = FileSystem.get(initialConf);
		Path in = new Path(otherArgs[0]);
		Path out = new Path(otherArgs[1]);				
		
		// Use old API for multiple outputs
		JobConf oldApiJob = new JobConf(initialConf);
		MultipleOutputs.addNamedOutput(oldApiJob, TRAINING_TAG,
				org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
				Text.class, VectorWritable.class);
		MultipleOutputs.addNamedOutput(oldApiJob, TEST_TAG,
				org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
				Text.class, VectorWritable.class);

		// Setup job with new API
		Job job = new Job(oldApiJob, className);
		job.setJarByClass(TrainAndTestVectorSplitter.class);// 主类
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		// job.setNumReduceTasks(1);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		job.submit();
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TrainAndTestVectorSplitter(), args);
	}

}
