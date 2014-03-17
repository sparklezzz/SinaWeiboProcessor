package sinaweibo.preprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * preprocess the keyword_class file: 
 * (1) add file type 1 to the second field
 * (2) write to sequential file
 * 
 */
public class KeywordClassPreprocessor {	
	
	public static void process(Path inputPath, Path outputPath, Configuration baseConf) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = baseConf;
		 String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		Job job = new Job(conf, className);	
		 
		 job.setJarByClass(KeywordClassPreprocessor.class);//主类
		 job.setMapperClass(KeywordClassPreprocessorMapper.class);//mapper
		 job.setReducerClass(Reducer.class);//reducer
		   
		 // map 输出Key的类型  
		 job.setMapOutputKeyClass(Text.class);  
		 // map输出Value的类型  
		 job.setMapOutputValueClass(Text.class);  
		 // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat  
		 job.setOutputKeyClass(Text.class);  
		 // reduce输出Value的类型  
		 job.setOutputValueClass(Text.class);  
		    
		 job.setInputFormatClass(TextInputFormat.class);  
		 // 提供一个RecordWriter的实现，负责数据输出。  
		 job.setOutputFormatClass(SequenceFileOutputFormat.class);             
		 
		 FileInputFormat.addInputPath(job, inputPath);//文件输入
		 FileOutputFormat.setOutputPath(job, outputPath);//文件输出
		 
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
		 		 
		 if (otherArgs.length < 2) {
		   System.err.println("Usage: " + className + " <indir> <outdir>");
		   System.exit(2);
		 }
		 
		 process(new Path(otherArgs[0]), new Path(otherArgs[1]), conf);
	}	
	
}
