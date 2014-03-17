package sinaweibo.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

public class GenSeparatePropertyChunk {

	static String getValidValues(JSONObject jsonObj, String key) {
		StringBuffer res = new StringBuffer();
		if (key.equals("id") || !jsonObj.has(key))
			return res.toString();
		Object valObj = jsonObj.get(key);
		if (valObj instanceof Integer) {
			Integer valInt = (Integer) valObj;
			if (valInt != 0)
				res.append(valInt.toString() + " ");
		} else if (valObj instanceof String) {

			String[] lst = ((String) valObj).split(" ");
			for (String s : lst) {
				if (!s.isEmpty() && !s.equals("0"))
					res.append(s + " ");
			}
		}
		if (res.length() > 0) {
			res = res.delete(res.length() - 1, res.length());
		}
		return res.toString();
	}

	private static List<Path> createProfileChunks(Path oriProfilePath,
			String oriProfilePrefix, Path profileChunkPathBase,
			Configuration baseConf, int recordPerChunk) throws IOException {
		List<Path> chunkPaths = Lists.newArrayList();

		Configuration conf = new Configuration(baseConf);

		FileSystem fs = FileSystem.get(oriProfilePath.toUri(), conf);

		int chunkIndex = 0;

		HashMap<String, SequenceFile.Writer> dictWriterMap = new HashMap<String, SequenceFile.Writer>();
		for (String property : GlobalName.PROFILE_PROPERTY_SET) {
			dictWriterMap.put(property, null);
		}

		for (Entry<String, SequenceFile.Writer> entry : dictWriterMap
				.entrySet()) {
			Path chunkPath = new Path(profileChunkPathBase,
					GlobalName.PROFILE_CHUNK_FILE_PREFIX + entry.getKey() + "_"
							+ chunkIndex);
			System.out.println("Creating chunk at " + chunkPath.toString());
			entry.setValue(new SequenceFile.Writer(fs, conf, chunkPath,
					Text.class, Text.class));
		}

		try {
			long currentChunkSize = 0;
			long recordNum = 0;
			long recordInCurChunk = 0;

			FileStatus[] status = fs.globStatus(new Path(oriProfilePath
					.toString(), oriProfilePrefix + "*"));
			Path[] listedPaths = FileUtil.stat2Paths(status);
			for (Path p : listedPaths) {
				FSDataInputStream fsis = fs.open(p);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fsis, "UTF8"));
				String line;
				Text lineText = new Text();
				while ((line = br.readLine()) != null) {
					if (recordInCurChunk == recordPerChunk) {
						chunkIndex++;
						for (Entry<String, SequenceFile.Writer> entry : dictWriterMap
								.entrySet()) {
							Closeables.closeQuietly(entry.getValue());
							Path chunkPath = new Path(profileChunkPathBase,
									GlobalName.PROFILE_CHUNK_FILE_PREFIX
											+ entry.getKey() + "_" + chunkIndex);
							System.out.println("Creating chunk at "
									+ chunkPath.toString());
							entry.setValue(new SequenceFile.Writer(fs, conf,
									chunkPath, Text.class, Text.class));
						}

						recordInCurChunk = 0;
					}
					int fieldSize = 4 + line.length() * 2;
					currentChunkSize += fieldSize;

					JSONObject jsonObj = new JSONObject(line.toString());
					if (!jsonObj.has("id"))
						continue;
					String id = jsonObj.getString("id");

					Iterator<?> propertyIter = jsonObj.keys();
					while (propertyIter.hasNext()) {
						String property = (String) propertyIter.next();
						if (!dictWriterMap.containsKey(property))
							continue;

						String values = getValidValues(jsonObj, property);
						if (values.length() > 0) {
							dictWriterMap.get(property).append(new Text(id),
									new Text(values));
						}
					}

					recordInCurChunk++;
					recordNum++;
					if (recordNum % 1000 == 0)
						System.out.println("Having write " + recordNum
								+ " users!");
				}

				br.close();
			}
			System.out
					.println("Having write " + recordNum + " users in total!");
		} finally {
			for (Entry<String, SequenceFile.Writer> entry : dictWriterMap
					.entrySet()) {
				Closeables.closeQuietly(entry.getValue());
			}
		}

		return chunkPaths;
	}

	public static void main(String[] args) throws IOException {
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
			System.err.println("Usage: " + className
					+ " <profile-indir> <profile-in-file-prefiex>"
					+ " <record num per chunk> <profile-chunks-outdir>");
			System.exit(2);
		}

		String oriProfilePathStr = otherArgs[0];
		String oriProfilePrefix = otherArgs[1];
		int chunkSizeInMegabytes = Integer.parseInt(otherArgs[2]);
		String profileChunkPathBaseStr = otherArgs[3];

		createProfileChunks(new Path(oriProfilePathStr), oriProfilePrefix,
				new Path(profileChunkPathBaseStr), conf, chunkSizeInMegabytes);

	}
}
