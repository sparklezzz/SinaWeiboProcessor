package sinaweibo.preprocessor;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeywordClassPreprocessorMapper extends Mapper<Object, Text, Text, Text> {
	private static final String TYPE_SEPARATOR = "\t";
	private final Text newKey = new Text();  
	  private final Text newVal = new Text();  
	  private static final String FILE_TYPE = "1";
	  
	  public void map(Object key, Text value, Context context)  
         throws IOException, InterruptedException {  
	 	  String line = value.toString();  
       int pos = line.indexOf("\t");

       if (pos != -1) {          	 
     	   newKey.set(line.substring(0, pos));
    	   newVal.set(FILE_TYPE + TYPE_SEPARATOR + line.substring(pos + 1));	     	         
           context.write(newKey, newVal);                            
       }  
    }  
}
