package sinaweibo.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import sinaweibo.preprocessor.TrainAndTestVectorRandomSplitter;
import sinaweibo.preprocessor.TrainAndTestVectorSplitter;
import sinaweibo.vectorizer.tfidf.TFIDFConverter;

public class SplitTrainAndTestVectorsOfAllProperties {

	public static void moveWildcardFilesToDir(FileSystem fs, String srcFilesWildcard, String dstDir) throws IOException {		
		FileStatus [] status = fs.globStatus(new Path(srcFilesWildcard));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p: listedPaths) {
			fs.rename(p, new Path(dstDir));
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void process(String []args) throws Exception {
		String inputPathStr = args[0];
		String trainRatioStr = args[1];
		
		String []subArgs = new String[3];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		for (String property : GlobalName.PROFILE_PROPERTY_SET) {
			System.out.println("Processing property " + property + " ...");
						
			String inDir = inputPathStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property;
			String tmpOutDir = inputPathStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_tmp";
			
			subArgs[0] = trainRatioStr;
			subArgs[1] = inDir;
			subArgs[2] = tmpOutDir;
			
			fs.delete(new Path(tmpOutDir));
			
			ToolRunner.run(new TrainAndTestVectorRandomSplitter(), subArgs);
			String trainPathStr = inputPathStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_train";
			String testPathStr = inputPathStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_test";
			fs.delete(new Path(trainPathStr));
			fs.delete(new Path(testPathStr));
			fs.mkdirs(new Path(trainPathStr));
			fs.mkdirs(new Path(testPathStr));
			
			moveWildcardFilesToDir(fs, tmpOutDir + "/train*", trainPathStr);
			moveWildcardFilesToDir(fs, tmpOutDir + "/test*", testPathStr);
			
			fs.delete(new Path(tmpOutDir));
		}
	}
	
	
	
	public static void main(String []args) throws Exception {
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
							+ " <weibo-vector-parent-indir> <train-ratio>");
			System.exit(2);
		}
		process(args);
	}
}
