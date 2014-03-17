package sinaweibo.preprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import sinaweibo.common.AbstractJob;

public class AllOriDataPreprocessor extends AbstractJob{

	private static final String KEYWORD_CLASS_TYPED_DIR = "keyword_class_typed";
	private static final String KEYWORD_TITLE_TYPED_DIR = "keyword_title_typed";
	private static final String KEYWORD_USER_TYPED_DIR = "keyword_user_typed";
	private static final String KEYWORD_CLASS_TITLE_COMBINED_DIR = "keyword_class_title_combined";
	private static final String KEYWORD_CLASS_USER_COMBINED_DIR = "keyword_class_user_combined";
	
	public static void main(String[] args) throws Exception {
	    ToolRunner.run(new AllOriDataPreprocessor(), args);
	  }
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = getConf();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		 String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		 		 
		 if (otherArgs.length < 5) {
		   System.err.println("Usage: " + className + " <class-indir> <titles-indir> <users-indir> <keyword-weight> <titles-weight> <output-base-dir>");
		   return -1;
		 }
		
		String classInputDir = otherArgs[0]; 
		String titleInputDir = otherArgs[1];
		String userInputDir = otherArgs[2];
		int keywordWeight = Integer.parseInt(otherArgs[3]);
		int contentWeight = Integer.parseInt(otherArgs[4]);
		String outputBaseDir = otherArgs[5];
		
		KeywordClassPreprocessor.process(new Path(classInputDir), 
				new Path(outputBaseDir + "/" + KEYWORD_CLASS_TYPED_DIR), conf);
		
		KeywordTitlePreprocessor.process(new Path(titleInputDir),
				new Path(outputBaseDir + "/" + KEYWORD_TITLE_TYPED_DIR), conf);
		
		KeywordUserPreprocessor.process(new Path(userInputDir), 
				new Path(outputBaseDir + "/" + KEYWORD_USER_TYPED_DIR), conf);
		
		KeywordClassAndTitleWeightCombiner.process(outputBaseDir + "/" + KEYWORD_CLASS_TYPED_DIR, 
				keywordWeight, 
				outputBaseDir + "/" + KEYWORD_TITLE_TYPED_DIR, 
				contentWeight, 
				outputBaseDir + "/" + KEYWORD_CLASS_TITLE_COMBINED_DIR
				, conf);
		
		KeywordClassAndTitleWeightCombiner.process(outputBaseDir + "/" + KEYWORD_CLASS_TYPED_DIR, 
				0, 
				outputBaseDir + "/" + KEYWORD_USER_TYPED_DIR, 
				1, 
				outputBaseDir + "/" + KEYWORD_CLASS_USER_COMBINED_DIR
				, conf);
		
		return 0;
	}
			
}
