package sinaweibo.preprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import sinaweibo.classifier.naivebayes.BayesUtils;
import sinaweibo.math.VectorWritable;
import sinaweibo.math.Vector;
import sinaweibo.util.MultipleInputs;


/*
 * Description: merge result by bayes and potential labels
 * 
 */

public class TitleBayesUserMergedClassifier {
	private static final String LABEL_INDEX_PATH = "LABEL_INDEX_PATH";
	private static final String FIRST_TYPE = "1";
	private static final String SECOND_TYPE = "2";
	
	public static class FirstMapper
    extends Mapper<Text, VectorWritable, Text, Text>{ 
	  private final Text newVal = new Text();  
	  private final StringBuffer m_buffer = new StringBuffer();
	  
	  public void map(Text key, VectorWritable value, Context context)  
           throws IOException, InterruptedException {  
	 	  m_buffer.delete(0, m_buffer.length());
		  m_buffer.append(FIRST_TYPE);
	 	  
		  Vector vector = value.get();
		  int vectorSize = vector.size();
		  
		  for (int i=0; i<vectorSize; ++i) {
			  m_buffer.append("\t" + vector.getQuick(i));
		  }
		  
		  newVal.set(m_buffer.toString());
		  context.write(key, newVal);	 
	  }
	}
		
	public static class SecondMapper
    extends Mapper<Text, Text, Text, Text>{
	  private final Text newVal = new Text();  
	  
	  public void map(Text key, Text value, Context context)  
           throws IOException, InterruptedException {  
	 	  		  
		  newVal.set(SECOND_TYPE + "\t" + value.toString());
		  context.write(key, newVal);	  		                           
	   }  
	}	
	
	public static class MyReducer
	extends Reducer<Text, Text, Text, Text> { 
		
		  private final Text newKey = new Text(); 
		  private final Text newVal = new Text();  
		  private final HashMap<Integer, Integer> m_potentialLabelIdx = new HashMap<Integer, Integer>();
		  private final ArrayList<Double> m_scoreArr = new ArrayList<Double>();
		  
		  private Map<Integer, String> m_labelIdx2Str = null;
		  private Map<String, Integer> m_labelStr2Idx = null;
		  
		  @Override
		  protected void setup(Context ctx) throws IOException, InterruptedException {
		    super.setup(ctx);
		    Configuration conf = ctx.getConfiguration();
		    String labelPath = conf.get(LABEL_INDEX_PATH);    
			 //load the labels
			 m_labelIdx2Str = BayesUtils.readLabelIndex(conf, new Path(labelPath));
			 m_labelStr2Idx = new HashMap<String, Integer>();
			 for (Entry<Integer, String> entry : m_labelIdx2Str.entrySet()) {
				 m_labelStr2Idx.put(entry.getValue(), entry.getKey());
			 }
		  } 
		  
		  
	   public void reduce(Text key, Iterable<Text> values,  
	           Context context) throws IOException, InterruptedException {            	      
		  m_potentialLabelIdx.clear();
		  m_scoreArr.clear();
		  double maxScore = -Double.MAX_VALUE;	// Note: Double.MIN_VALUE is greater than 0!
		  int maxIdx = 0;
		   
		  String keyStr = key.toString();
		  String []lst = keyStr.split("/");
		  if (lst.length < 3)	return;
		  newKey.set(lst[2]);
		  
		  if (!lst[1].equals("-")) {
			  newVal.set(lst[1]);	// training case
			  //context.write(newKey, newVal);
			  return;
		  }
		  
		  // test case
	 	  for (Text val : values) {
	 		  String valStr = val.toString();
	 		  lst = valStr.split("\t");
	 		  	 		  
	 		  if (lst.length < 2)
	 			  continue;
	 		  
	 		  if (lst[0].equals(FIRST_TYPE)) {
	 			  for (int i=1; i<lst.length; ++i) {
	 				  if (lst[i].trim().isEmpty())
	 			  		  continue;
	 				  double tmpScore = Double.parseDouble(lst[i].trim());
	 				  if (maxScore < tmpScore) {
	 					  maxIdx = i-1;
	 					  maxScore = tmpScore;
	 				  }
	 				  m_scoreArr.add(tmpScore);
	 			  }
	 		  } else if (lst[0].equals(SECOND_TYPE)) {
	 			  for (int i=1; i<lst.length; i += 2) {
	 				  if (i+1 == lst.length)
	 					  break;
	 				 m_potentialLabelIdx.put(m_labelStr2Idx.get(lst[i]), Integer.parseInt(lst[i+1]));
	 			  }
	 		  } 
	      }
	 	  
	 	  if (m_potentialLabelIdx.size() == 0) {	// no potential label
	 		  newVal.set(m_labelIdx2Str.get(maxIdx));
	 	  } else {
	 		  /*
	 		   * for each potential label, choose one with max label count
	 		   * if two labels have same label count, choose one with larger prob in bayes result
	 		   */	 	
	 		  int maxLabelCount = -1;
	 		  int maxLabelIdx = Integer.MIN_VALUE;
	 		  for (Entry<Integer, Integer> entry : m_potentialLabelIdx.entrySet()) {
	 			  if (	entry.getValue() > maxLabelCount ||
	 					(entry.getValue() == maxLabelCount && (m_scoreArr.size() > 0 && m_scoreArr.get(entry.getKey()) > m_scoreArr.get(entry.getKey())))) {
	 				  maxLabelCount = entry.getValue();
	 				  maxLabelIdx = entry.getKey();
	 			  }
	 		  }
	 		  newVal.set(m_labelIdx2Str.get(maxLabelIdx) + "\t" + m_labelIdx2Str.get(maxIdx));
	 		 context.write(newKey, newVal);	 	  
	 	  }
	 		  
	 	  //context.write(newKey, newVal);	 	  
	   }     
	}	
	
	private static void runMapReduceJob(Configuration conf, Path path, Path path2, Path path3, String labelIndexPath) 
			throws IOException, InterruptedException, ClassNotFoundException {
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
		 
		 job.setJarByClass(TitleBayesUserMergedClassifier.class);//主类
		 job.setMapperClass(FirstMapper.class);//mapper
		 job.setReducerClass(MyReducer.class);//reducer
		   
		 // map 输出Key的类型  
		 job.setMapOutputKeyClass(Text.class);  
		 // map输出Value的类型  
		 job.setMapOutputValueClass(Text.class);  
		 // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat  
		 job.setOutputKeyClass(Text.class);  
		 // reduce输出Value的类型  
		 job.setOutputValueClass(Text.class);  
		    		  
		 job.setOutputFormatClass(TextOutputFormat.class); 
		 
		 MultipleInputs.addInputPath(job, path, SequenceFileInputFormat.class, FirstMapper.class);
		 MultipleInputs.addInputPath(job, path2, SequenceFileInputFormat.class, SecondMapper.class);
		 FileOutputFormat.setOutputPath(job, path3);//文件输出
	    boolean succeeded = job.waitForCompletion(true);
	    if (!succeeded) {
	      throw new IllegalStateException("MR Job failed!");
	    }
	}	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("deprecation")
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
		 		 
		 if (otherArgs.length < 4) {
		   System.err.println("Usage: " + className + " <label-index-input> <keyword-class-title-classified-input> <user-potential-label-set-input> <outdir>");
		   System.exit(2);
		 }
		 	
		 runMapReduceJob(conf, new Path(otherArgs[1]), new Path(otherArgs[2]), new Path(otherArgs[3]), otherArgs[0]);

	}

}

