package sinaweibo.preprocessor;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
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

import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;
import sinaweibo.math.Vector.Element;

/*
 * 
 * 
 * 
 */

public class Vector2TextConverter {
	
	public static class MyMapper
    extends Mapper<Text, VectorWritable, Text, Text>{	  	  
	  private static final Text newVal = new Text();  
	  private static final StringBuffer m_buffer = new StringBuffer(); 	
		
	  public void map(Text key, VectorWritable value, Context context)  
           throws IOException, InterruptedException {  
	 	  
		  m_buffer.delete(0, m_buffer.length());
		  
		  Vector vector = value.get();
		  int nonZeroCount = vector.getNumNondefaultElements();
		  
		  m_buffer.append(nonZeroCount);
		  
		  Iterator<Element> iter = vector.iterateNonZero();
		  while (iter.hasNext()) {
		      Element e = iter.next();
		      m_buffer.append("\t" + e.index() + "\t" + e.get());
		    }
		  
		  newVal.set(m_buffer.toString());		  		  			 
		  context.write(key, newVal);	// value is empty	                        
	   }  
	}	
	

	private static void runMapReduceJob(Configuration conf, Path path, Path path2) throws IOException, InterruptedException, ClassNotFoundException {
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
		 
		 job.setJarByClass(Vector2TextConverter.class);//主类
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
		    
		 job.setInputFormatClass(SequenceFileInputFormat.class);    
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
		 	
		 runMapReduceJob(conf, new Path(otherArgs[0]), new Path(otherArgs[1]));
	}

}

