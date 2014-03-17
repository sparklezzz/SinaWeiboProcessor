package sinaweibo.preprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * 
 * Description: Extract keyword with only number and alphabetics 
 * 
 */

public class ASCIIKeywordExtractor {

	private static final String MIN_ASCII_THRESHOLD = "MIN_ASCII_THRESHOLD";
	
	public static class MyMapper
    extends Mapper<Text, Text, Text, Text>{	  
	  private static float MIN_THRESHOLD = (float) 0.8;
	  
	  @Override
	  protected void setup(Context ctx) throws IOException, InterruptedException {
	    super.setup(ctx);
	    Configuration conf = ctx.getConfiguration();
	    MIN_THRESHOLD = conf.getFloat(MIN_ASCII_THRESHOLD, (float) 0.8);
	  }
	  
	  public void map(Text key, Text value, Context context)  
           throws IOException, InterruptedException {  
	 	  
		  String keyStr = key.toString();
		  String []lst = keyStr.split("/");
		  if (lst.length < 3)	return;
		  
		  int keywordLen = lst[2].length();	
		  int asciiCount = 0;
		  
		  for (Character c: lst[2].toCharArray()) {
			  if ((int)c > 0 && (int) c < 128 ) {
				  asciiCount ++;
			  }
		  }
		  
		  if (asciiCount / (float)keywordLen > MIN_THRESHOLD) {
			  if (!lst[1].equals("-")) {
				  context.getCounter("MyCounter", "TrainCaseExtractedCount").increment(1);
			  }			  			 
			  context.write(key, value);	// value is empty
		  }		                        
	   }  
	}	
	
	public static class MySortComparator extends WritableComparator {  
        protected MySortComparator() {  
            super(Text.class, true);  
        }  
  
        @SuppressWarnings("rawtypes")  
        @Override  
        public int compare(WritableComparable w1, WritableComparable w2) {  
        	String []lst1 = w1.toString().split("/");
        	String []lst2 = w1.toString().split("/");
        	
        	if (lst1.length < 3 || lst2.length < 3) {
        		return w1.toString().compareTo(w2.toString());
        	}
        	
            return lst1[2].compareTo(lst2[2]);
        }  
    }
	
	
	// Extract valid ascii keyword
	private static void runMapReduceJob(Configuration conf, Path path, Path path2, float min_threshold) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		conf.setFloat(MIN_ASCII_THRESHOLD, min_threshold);
		
		String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		
		 Job job = new Job(conf, className);
		 job.setNumReduceTasks(1);
		 
		 job.setJarByClass(ASCIIKeywordExtractor.class);//主类
		 job.setMapperClass(MyMapper.class);//mapper
		 job.setReducerClass(Reducer.class);//reducer
		   
		 job.setSortComparatorClass(MySortComparator.class);
		 
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
		 FileInputFormat.addInputPath(job, path);//文件输入
		 FileOutputFormat.setOutputPath(job, path2);//文件输出
	    boolean succeeded = job.waitForCompletion(true);
	    if (!succeeded) {
	      throw new IllegalStateException("Job failed!");
	    }
	}	    
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		 String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		 		 
		 if (otherArgs.length < 3) {
		   System.err.println("Usage: " + className + " <min ascii threshold between 0 and 1> <indir> <outdir>");
		   System.exit(2);
		 }
		 	
		 runMapReduceJob(conf, new Path(otherArgs[1]), new Path(otherArgs[2]), Float.parseFloat(otherArgs[0]));
	}

}
