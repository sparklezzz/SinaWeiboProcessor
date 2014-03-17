package sinaweibo.preprocessor;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.LineReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import sinaweibo.vectorizer.MahoutChineseAnalyzer;

/*
 * 功能：对每个未分类的关键字：先切词，然后对其中每个词归类。若其中被归类的词都被归到一个类，则将整个关键字归到那个类中；
 *      若被归类到多个类，则不对这个关键词分类。
 * 字典存储在hdfs上，每一行格式：词\t所属label\t词频
 * 
 */


public class KeywordClassifierByDict {
	private static final String DICT_PATH = "DICT_PATH";
	
	public static class MyMapper
    extends Mapper<Object, Text, Text, Text>{
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();
	  private Map<String, String> m_dictMap = null;
	  private Analyzer m_analyzer = null;	  
	  private Set<String> m_curLabelSet = new HashSet<String>();
	  
	  @Override
	  protected void setup(Context ctx) throws IOException, InterruptedException {
	    super.setup(ctx);
	    Configuration conf = ctx.getConfiguration();
	    String dictPathStr = conf.get(DICT_PATH);    
		 //load the labels
	    m_dictMap = new HashMap<String, String>();
	    Path dictPath = new Path(dictPathStr);
	    FileSystem fs = dictPath.getFileSystem(conf);
	    FSDataInputStream dis = fs.open(dictPath);
	    LineReader in = new LineReader(dis, conf);
	    Text line = new Text();
	    while (in.readLine(line) > 0) {
	    	String []lst = line.toString().trim().split("\t");
	    	if (lst.length < 2)
	    		continue;
	    	m_dictMap.put(lst[0], lst[1]);
	    }
	    
	    dis.close();
	    in.close();	 
	    
	    m_analyzer = new MahoutChineseAnalyzer();
	  }
	  
	  public void map(Object key, Text value, Context context)  
	         throws IOException, InterruptedException {  
		 	  String line = value.toString();  
	       int pos = line.indexOf("\t");
	
	       if (pos != -1) {	    	 
	     	   newKey.set(line.substring(0, pos));
	     	   String tmp = line.substring(pos+1);
	     	   if (tmp.equals("-")) {	// only process test data
	     		   // tokenize term
	     		    TokenStream stream = m_analyzer.reusableTokenStream(newKey.toString(), new StringReader(newKey.toString()));
	     		    CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
	     		    stream.reset();
	     		    m_curLabelSet.clear();	     		    
	     		    while (stream.incrementToken()) {
	     		      if (m_dictMap.containsKey(termAtt.toString()))	 {
	     		    	 m_curLabelSet.add(m_dictMap.get(termAtt.toString()));
	     		      }
	     		    }
	     		    
	     		    if (m_curLabelSet.size() == 1) {	// all labeled terms has same label
	     		    	for (String labelStr : m_curLabelSet) {
	     		    		tmp = labelStr;
	     		    	}
	     		    	context.getCounter("MyCounter", "TestCaseLabeledCounter").increment(1);
	     		    }
	     	   }	     	   
	     	   newVal.set(tmp);
	           context.write(newKey, newVal);                            
	       }  
	    }  	  	  
	}	
	
	private static void process(Configuration conf, Path path, Path path2, String dictPath) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		conf.set(DICT_PATH, dictPath);
		
		String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
		
		 Job job = new Job(conf, className);
		 job.setNumReduceTasks(1);
		 
		 job.setJarByClass(KeywordClassifierByDict.class);//主类
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
		 		 
		 if (otherArgs.length < 3) {
		   System.err.println("Usage: " + className + " <dict-file-path-on-hdfs> <indir> <outdir>");
		   System.exit(2);
		 }
		 
		 
		 process(conf, new Path(otherArgs[1]), new Path(otherArgs[2]), otherArgs[0]);
	}
}
