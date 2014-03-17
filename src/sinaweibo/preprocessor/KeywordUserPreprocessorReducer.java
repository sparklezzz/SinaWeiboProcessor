package sinaweibo.preprocessor;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeywordUserPreprocessorReducer
extends Reducer<Text, Text, Text, Text> { 	       
	  private static final String TYPE_SEPARATOR = "\t";
	  private static final String FILE_TYPE = "2";
	  private final Text newVal = new Text();  
	  private final StringBuffer buffer = new StringBuffer();
  
   public void reduce(Text key, Iterable<Text> values,  
           Context context) throws IOException, InterruptedException {            	      
	  buffer.delete(0, buffer.length());
	  buffer.append(FILE_TYPE + TYPE_SEPARATOR);
	  
 	  for (Text val : values) {
 		  buffer.append(val.toString() + "\t");
 		  context.write(key, val);
       }
 	  if (buffer.length() > 0) {
 		  buffer.delete(buffer.length()-1, buffer.length());
 	  }
 	  newVal.set(buffer.toString());
 	  context.write(key, newVal);
   }     
}
