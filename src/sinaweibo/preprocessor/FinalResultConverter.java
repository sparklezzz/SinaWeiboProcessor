package sinaweibo.preprocessor;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import sinaweibo.classifier.naivebayes.BayesUtils;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;

public class FinalResultConverter {

	private static final String LABEL_INDEX_PATH = "LABEL_INDEX_PATH";
	
	public static class MyMapper
    extends Mapper<Text, VectorWritable, Text, Text>{
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();
	  private Map<Integer, String> labelMap = null;
	  
	  @Override
	  protected void setup(Context ctx) throws IOException, InterruptedException {
	    super.setup(ctx);
	    Configuration conf = ctx.getConfiguration();
	    String labelPath = conf.get(LABEL_INDEX_PATH);    
		 //load the labels
		 labelMap = BayesUtils.readLabelIndex(conf, new Path(labelPath));
	  }
	  
	  public void map(Text key, VectorWritable value, Context context)  
           throws IOException, InterruptedException {  
	 	  
		  String keyStr = key.toString();
		  String []lst = keyStr.split("/");
		  if (lst.length < 3)	return;
		  newKey.set(lst[2]);
		  
		  if (!lst[1].equals("-")) {
			  newVal.set(lst[1]);	// training result
		  } else {
			  int bestIdx = Integer.MIN_VALUE;
		      double bestScore = Long.MIN_VALUE;
		      for (Vector.Element element : value.get()) {
		        if (element.get() > bestScore) {
		          bestScore = element.get();
		          bestIdx = element.index();
		        }
		      }
		      if (bestIdx == Integer.MIN_VALUE) {
		        return;
		      }
		      newVal.set(labelMap.get(bestIdx));
		  }
		  		  
	      context.write(newKey, newVal);                            
	   }  
	}	
	
	private static void process(Configuration conf, Path path, Path path2, String labelIndexPath) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		conf.set(LABEL_INDEX_PATH, labelIndexPath);
		
		String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		
		 Job job = new Job(conf, className);
		 job.setNumReduceTasks(1);
		 
		 job.setJarByClass(FinalResultConverter.class);//主类
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
		 		 
		 if (otherArgs.length < 3) {
		   System.err.println("Usage: " + className + " <label-index-path> <indir> <outdir>");
		   System.exit(2);
		 }
		 
		 
		 process(conf, new Path(otherArgs[1]), new Path(otherArgs[2]), otherArgs[0]);
	}

}
