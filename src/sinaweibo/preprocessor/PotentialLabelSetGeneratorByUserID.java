package sinaweibo.preprocessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
 * Description:
 * 利用keyword-label-userid文件，取得每个keyword可能属于的label集合
 * 思想：利用keyword和userid之间的二部图关系，对于每个未标记的keyword，
 * 找到和它通过userid有一度连接的有label的keyword集合，统计其中每个label及其权重（被userid连接到的次数）
 * 
 */

public class PotentialLabelSetGeneratorByUserID {

	/*
	 *  Mapper for first MR
	 *  </label/keyword, list<userid> > --> list <userid, /label/keyword>
	 */
	public static class FirstMapper
    extends Mapper<Text, Text, Text, Text>{
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();  
	  
	  public void map(Text key, Text value, Context context)  
           throws IOException, InterruptedException {  
	 	  		  
		  String keyStr = key.toString();
		  String []valueLst = value.toString().split("/");		  
		  newVal.set(keyStr);
		  for (String userID : valueLst) {
			  if (userID == null || userID.trim().isEmpty())
				  continue;
			  newKey.set(userID.trim());
			  context.write(newKey, newVal);
		  }		  		                           
	   }  
	}	
	
	/*
	 * Reducer for first MR
	 * <userid, list</label/keyword> > ---> list </unlabeled/keyword1, another_label>
	 */
	public static class FirstReducer
	extends Reducer<Text, Text, Text, Text> { 	 
		  private final Text newKey = new Text();
		  private final Text newVal = new Text();  
		  private final HashSet<String> m_unlabeledSet = new HashSet<String>();
		  private final HashSet<String> m_labeledSet = new HashSet<String>();
	    
	   public void reduce(Text key, Iterable<Text> values,  
	           Context context) throws IOException, InterruptedException {            	      
		   
		   m_unlabeledSet.clear();
		   m_labeledSet.clear();
		   
	 	  for (Text val : values) {
	 		  String valStr = val.toString();
	 		  String []lst = valStr.split("/");
	 		  if (lst.length < 3 || lst[1] == null || lst[1].isEmpty())
	 			  continue;
	 		  if (lst[1].equals("-"))
	 			  m_unlabeledSet.add(valStr);
	 		  else
	 			  m_labeledSet.add(valStr);	 		 
	      }
	 	  
	 	  for (String elem1: m_unlabeledSet) {
	 		  for (String elem2: m_labeledSet) {
	 			  newKey.set(elem1);
	 			  newVal.set(elem2.split("/")[1]);	// add label only
	 			  context.write(newKey, newVal);
	 		  }
	 	  }
	   }     
	}	
	
	/*
	 * Reducer for second MR
	 * </unlabeled/keyword, list<label>> ---> </unlabeled/keyword, map<label, count>>
	 */
	public static class SecondReducer
	extends Reducer<Text, Text, Text, Text> { 	 
		  private final Text newVal = new Text();  
		  private final HashMap<String, Integer> m_labelMap = new HashMap<String, Integer>();
	      private final StringBuffer m_buffer = new StringBuffer();
	      
	   public void reduce(Text key, Iterable<Text> values,  
	           Context context) throws IOException, InterruptedException {            	      
		   m_labelMap.clear();
		   m_buffer.delete(0, m_buffer.length());		
		   
	 	  for (Text val : values) {
	 		  String valStr = val.toString().trim();
	 		  if (valStr == null || valStr.trim().isEmpty())
	 			  continue;
	 		  valStr = valStr.trim();
	 		  if (m_labelMap.containsKey(valStr)) {
	 			  m_labelMap.put(valStr, m_labelMap.get(valStr) + 1);
	 		  } else {
	 			  m_labelMap.put(valStr, 1);
	 		  }
	      }
	 	  
	 	  for (Entry<String, Integer> entry: m_labelMap.entrySet()) {
	 		  m_buffer.append(entry.getKey() + "\t" + entry.getValue().toString() + "\t");
	 	  }
	 	  newVal.set(m_buffer.toString());
	 	  context.write(key, newVal);
	   }     
	}	
	
	private static void runMapReduceJob1(Configuration conf, Path path, Path path2) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		
		 Job job = new Job(conf, className);
		 job.setNumReduceTasks(4);
		 
		 job.setJarByClass(PotentialLabelSetGeneratorByUserID.class);//主类
		 job.setMapperClass(FirstMapper.class);//mapper
		 job.setReducerClass(FirstReducer.class);//reducer
		   
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
	      throw new IllegalStateException("MR Job1 failed!");
	    }
	}	
		
	private static void runMapReduceJob2(Configuration conf, Path path, Path path2) throws IOException, InterruptedException, ClassNotFoundException {
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
		 
		 job.setJarByClass(PotentialLabelSetGeneratorByUserID.class);//主类
		 job.setMapperClass(Mapper.class);//mapper
		 job.setReducerClass(SecondReducer.class);//reducer
		   
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
	      throw new IllegalStateException("MR Job2 failed!");
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
		 		 
		 if (otherArgs.length < 2) {
		   System.err.println("Usage: " + className + " <keyword-class-userid-combined-indir> <outdir>");
		   System.exit(2);
		 }
		 	
		 String tmpDir = otherArgs[1] + "_temp";
		 FileSystem.get(conf).delete(new Path(tmpDir));
		 runMapReduceJob1(conf, new Path(otherArgs[0]), new Path(tmpDir));
		 runMapReduceJob2(conf, new Path(tmpDir), new Path(otherArgs[1]));
		 //FileSystem.get(conf).delete(new Path(tmpDir));
	}

}
