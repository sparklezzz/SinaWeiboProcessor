package sinaweibo.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;
import sinaweibo.math.Vector.Element;

public class Vector2LibSVMFormatConverter {

	public static void writeData(Configuration conf, Path vectorDirPath,
			BufferedWriter bw) throws IOException {
		System.out.println("Writing vectors...");

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(vectorDirPath.toString(),
				"*"));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		TreeMap<Integer, Double> tmpVec = new TreeMap<Integer, Double>(); 
		int count = 0;
		for (Path p : listedPaths) {
			if (p.getName().toString().startsWith("_"))
				continue;
			System.out.println("Opening " + p.toString() + " ...");
			for (Pair<Text, VectorWritable> record : new SequenceFileIterable<Text, VectorWritable>(
					p, true, conf)) {
				tmpVec.clear();

				Vector vector = record.getSecond().get();
				Iterator<Element> iter = vector.iterateNonZero();
				while (iter.hasNext()) {
					Element e = iter.next();
					// Note that the index of svm file starts from 1!
					tmpVec.put(e.index() + 1, e.get());
				}

				if (tmpVec.size() == 0)
					continue;

				String label = record.getFirst().toString().split("/")[1];
				if (label != null && !label.equals("null")
						&& !label.equals("0") && !label.equals("-")
						&& !label.isEmpty()) {
					// write label
					bw.write(label);
					// write each dimension
					for (Entry<Integer, Double> entry : tmpVec.entrySet()) {

						bw.write(" " + entry.getKey().toString() + ":"
								+ entry.getValue().toString());
					}
					bw.write("\n");
				}

				count++;
				if (count % 10000 == 0) {
					System.out.println("Having wrote " + count + " vectors!");
				}
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws Exception {
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
			System.err.println("Usage: " + className
					+ " <input-dir> <local-outdir>");
			System.exit(2);
		}

		String inputDirStr = otherArgs[0];
		String localOutDirStr = otherArgs[1];
		File localOutDir = new File(localOutDirStr);
		localOutDir.mkdirs();
		if (!localOutDir.isDirectory()) {
			throw new Exception("Cannot mkdir: " + localOutDirStr);
		}

		for (String property: GlobalName.PROFILE_PROPERTY_SET) {
			// write train data
			String localTrainFilePathStr = localOutDirStr + "/" + property + "_train";
			System.out.println("Writing to local train file " + localTrainFilePathStr + " ...");
			BufferedWriter bw = new BufferedWriter(new FileWriter(
					localTrainFilePathStr));					
			writeData(conf, new Path(inputDirStr, GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_train"), 
					 bw);
			bw.close();
		}
		
		for (String property: GlobalName.PROFILE_PROPERTY_SET) {
			// write test data
			String localTestFilePathStr = localOutDirStr + "/" + property + "_test";
			System.out.println("Writing to local train file " + localTestFilePathStr + " ...");
			BufferedWriter bw = new BufferedWriter(new FileWriter(
					localTestFilePathStr));					
			writeData(conf, new Path(inputDirStr, GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_test"), 
					 bw);
			bw.close();
		}
	}

}
