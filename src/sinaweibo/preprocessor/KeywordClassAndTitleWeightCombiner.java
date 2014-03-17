package sinaweibo.preprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * combine the keyword_class and keyword_title/userid file: 
 * can set weights for keyword and title
 * read from/write to sequential file
 * 
 * 
 */
public class KeywordClassAndTitleWeightCombiner {	
	private static final String FIRST_WEIGHT = "FIRST_WEIGHT";
	private static final String SECOND_WEIGHT = "SECOND_WEIGHT";	
	
	public static void process(String firstInput, int firstWeight, String secondInput, 
			int secondWeight, String output, Configuration baseConf) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = baseConf;
		String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();					 
		 
		 Job job = new Job(conf, className);	
		 job.getConfiguration().setInt(FIRST_WEIGHT, firstWeight);
		 job.getConfiguration().setInt(SECOND_WEIGHT, secondWeight);
		 
		 job.setJarByClass(KeywordClassAndTitleWeightCombiner.class);//主类
		 job.setMapperClass(Mapper.class);//mapper
		 job.setReducerClass(KeywordClassAndTitleWeightCombinerReducer.class);//reducer
		   
		 // map 输出Key的类型  
		 job.setMapOutputKeyClass(Text.class);  
		 // map输出Value的类型  
		 job.setMapOutputValueClass(Text.class);  
		 // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat  
		 job.setOutputKeyClass(Text.class);  
		 // reduce输出Value的类型  
		 job.setOutputValueClass(Text.class);  
		    
		 job.setInputFormatClass(SequenceFileInputFormat.class);  
		 // 提供一个RecordWriter的实现，负责数据输出。  
		 job.setOutputFormatClass(SequenceFileOutputFormat.class); 
		 
		 FileInputFormat.addInputPaths(job, firstInput + "," + secondInput);//文件输入
		 FileOutputFormat.setOutputPath(job, new Path(output));//文件输出
		 boolean succeeded = job.waitForCompletion(true);
		 if (!succeeded) 
		      throw new IllegalStateException("Job failed!");
	}
	
	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		 String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		 		 
		 if (otherArgs.length < 5) {
		   System.err.println("Usage: " + className + " <first-indir> <first-indir> <second-indir> <second-weight> <outdir>");
		   System.exit(2);
		 }
		 
		 int firstWeight = Integer.parseInt(otherArgs[1]);
		 int secondWeight = Integer.parseInt(otherArgs[3]);		
		 
		 process(otherArgs[0], firstWeight, otherArgs[2], secondWeight, otherArgs[4], conf);		 
	}	
	
}
