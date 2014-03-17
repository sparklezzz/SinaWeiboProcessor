package sinaweibo.preprocessor;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class RandomClassifier {
	
	public static class MyMapper
    extends Mapper<Object, Text, Text, Text>{
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();
	  private final Random r = new Random();
	  	  
	  public void map(Object key, Text value, Context context)  
           throws IOException, InterruptedException {  
	 	  
		  String line = value.toString();  
	       int pos = line.indexOf("\t");
	
	       if (pos != -1) {          	 
	     	   newKey.set(line.substring(0, pos));
	    	   String tmp = line.substring(pos + 1);
	    	   if (tmp.equals("-")) {
	    		   int ran = Math.abs(r.nextInt()) % 33 + 1;
	    		   tmp = new Integer(ran).toString();
	    	   }	     	   
	     	   newVal.set(tmp);
	     	         
	           context.write(newKey, newVal);                            
	       }                           
	   }  
	}	
	
	private static void process(Configuration conf, Path path, Path path2) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		
		 Job job = new Job(conf, className);
		 job.setNumReduceTasks(1);
		 
		 job.setJarByClass(RandomClassifier.class);//主类
		 job.setMapperClass(MyMapper.class);//mapper
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
		 job.setOutputFormatClass(TextOutputFormat.class); 
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
		 		 
		 if (otherArgs.length < 2) {
		   System.err.println("Usage: " + className + " <indir> <outdir>");
		   System.exit(2);
		 }
		 
		 process(conf, new Path(otherArgs[0]), new Path(otherArgs[1]));
	}

}
