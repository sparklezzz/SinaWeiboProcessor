package sinaweibo.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

public class SampleUIDFromProfile {

	private static List<Path> ExtractUID(Path oriProfilePath,
			String oriProfilePrefix, Path UIDChunkPathBase,
			Configuration baseConf, float ratio) throws IOException {
		List<Path> chunkPaths = Lists.newArrayList();

		Configuration conf = new Configuration(baseConf);

		FileSystem fs = FileSystem.get(oriProfilePath.toUri(), conf);

		int chunkSizeInMegabytes = Integer.MAX_VALUE;
		long chunkSizeLimit = chunkSizeInMegabytes * 1024L * 1024L;
		int chunkIndex = 0;
		Path chunkPath = new Path(UIDChunkPathBase,
				GlobalName.UID_CHUNK_FILE_PREFIX + chunkIndex);
		System.out.println("Creating chunk at " + chunkPath.toString());
		chunkPaths.add(chunkPath);

		SequenceFile.Writer dictWriter = new SequenceFile.Writer(fs, conf,
				chunkPath, Text.class, NullWritable.class);

		try {
			long currentChunkSize = 0;
			long recordNum = 0;

			Text idText = new Text();
			FileStatus[] status = fs.globStatus(new Path(oriProfilePath
					.toString(), oriProfilePrefix + "*"));
			Path[] listedPaths = FileUtil.stat2Paths(status);
			for (Path p : listedPaths) {
				FSDataInputStream fsis = fs.open(p);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fsis, "UTF8"));
				String line;
				while ((line = br.readLine()) != null) {
					if (currentChunkSize > chunkSizeLimit) {
						Closeables.closeQuietly(dictWriter);
						chunkIndex++;

						chunkPath = new Path(UIDChunkPathBase,
								GlobalName.UID_CHUNK_FILE_PREFIX + chunkIndex);
						System.out.println("Creating chunk at "
								+ chunkPath.toString());
						chunkPaths.add(chunkPath);

						dictWriter = new SequenceFile.Writer(fs, conf,
								chunkPath, Text.class, NullWritable.class);
						currentChunkSize = 0;
					}

					if (Math.random() >= ratio)
						continue;

					JSONObject jsonObj = new JSONObject(line.toString());
					if (!jsonObj.has("id"))
						continue;
					String id = jsonObj.getString("id");
					idText.set(id);

					int fieldSize = 4 + id.length() * 2;
					currentChunkSize += fieldSize;

					dictWriter.append(idText, NullWritable.get());
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
			Closeables.closeQuietly(dictWriter);
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
					+ " <ratio> <outdir>");
			System.exit(2);
		}

		String oriProfilePathStr = otherArgs[0];
		String oriProfilePrefix = otherArgs[1];
		float ratio = Float.parseFloat(otherArgs[2]);
		String profileChunkPathBaseStr = otherArgs[3];

		ExtractUID(new Path(oriProfilePathStr), oriProfilePrefix, new Path(
				profileChunkPathBaseStr), conf, ratio);

	}

}
