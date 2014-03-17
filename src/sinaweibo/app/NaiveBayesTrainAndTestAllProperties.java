package sinaweibo.app;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import sinaweibo.classifier.naivebayes.test.TestNaiveBayesDriver;
import sinaweibo.classifier.naivebayes.training.TrainNaiveBayesJob;
import sinaweibo.preprocessor.TrainAndTestVectorSplitter;

/*
 * For each property, do train and test naive bayes job
 * 
 */

public class NaiveBayesTrainAndTestAllProperties {
	
	public static void process(String []args) throws Exception {
		Configuration conf = new Configuration();
		String parentStr = args[0];
		FileSystem fs = FileSystem.get(conf);
		
		//GlobalName.PROFILE_PROPERTY_SET.clear();
		//GlobalName.PROFILE_PROPERTY_SET.add("norm_所在地省级");
						
		
		
		System.out.println("Training...");
		
		for (String property : GlobalName.PROFILE_PROPERTY_SET) {
			System.out.println("Processing property " + property + " ...");
			
			ArrayList<String> buffer = new ArrayList<String>();
			buffer.add("-Dmapred.child.java.opts=-Xmx4096m");
			
			String inStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_train";
			buffer.add("-i");
			buffer.add(inStr);
			String outStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_model";
			buffer.add("-o");
			buffer.add(outStr);
			String labelPathStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_label_index";
			buffer.add("-li");
			buffer.add(labelPathStr);
			buffer.add("-el");		
			if (args.length > 1) {
				buffer.add("-a");
				buffer.add(args[1]);
			}
			buffer.add("-ow");	// delete last output and tmp path
			
			String []subArgs = buffer.toArray(new String[0]);
			
			ToolRunner.run(new TrainNaiveBayesJob(), subArgs);			
		}
		
		System.out.println("Testing on train data...");
		for (String property : GlobalName.PROFILE_PROPERTY_SET) {
			System.out.println("Processing property " + property + " ...");
			
			ArrayList<String> buffer = new ArrayList<String>();
			buffer.add("-Dmapred.child.java.opts=-Xmx4096m");
			String inStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_train";
			buffer.add("-i");
			buffer.add(inStr);
			String modelStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_model";
			buffer.add("-m");
			buffer.add(modelStr);
			String labelPathStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_label_index";
			buffer.add("-l");
			buffer.add(labelPathStr);
			String outStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_train_evaluation";
			buffer.add("-o");
			buffer.add(outStr);
			
			buffer.add("-p");
			
			buffer.add("-ow");	// delete last output and tmp path
			
			String []subArgs = buffer.toArray(new String[0]);
			
			ToolRunner.run(new TestNaiveBayesDriver(), subArgs);			
		}
		
		System.out.println("Testing on test data...");
		for (String property : GlobalName.PROFILE_PROPERTY_SET) {
			System.out.println("Processing property " + property + " ...");
			
			ArrayList<String> buffer = new ArrayList<String>();
			buffer.add("-Dmapred.child.java.opts=-Xmx4096m");
			String inStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_test";
			buffer.add("-i");
			buffer.add(inStr);
			String modelStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_model";
			buffer.add("-m");
			buffer.add(modelStr);
			String labelPathStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_label_index";
			buffer.add("-l");
			buffer.add(labelPathStr);
			String outStr = parentStr + "/" + GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property + "_test_evaluation";
			buffer.add("-o");
			buffer.add(outStr);
						
			buffer.add("-p");
			
			buffer.add("-ow");	// delete last output and tmp path
			
			String []subArgs = buffer.toArray(new String[0]);
			
			ToolRunner.run(new TestNaiveBayesDriver(), subArgs);			
		}
		
	}
	
	
	public static void main(String []args) throws Exception {
		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		if (args.length < 1) {
			System.err
					.println("Usage: "
							+ className
							+ " <weibo-vector-parent-indir>");
			System.exit(2);
		}
		process(args);
		
	}
}
