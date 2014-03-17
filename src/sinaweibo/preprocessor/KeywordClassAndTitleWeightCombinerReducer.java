package sinaweibo.preprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeywordClassAndTitleWeightCombinerReducer extends Reducer<Text, Text, Text, Text> { 
	private static final String TYPE_SEPARATOR = "\t";
	private static final String FIRST_WEIGHT = "FIRST_WEIGHT";
	private static final String SECOND_WEIGHT = "SECOND_WEIGHT";
	
	private final Text newKey = new Text();
	  private final Text newVal = new Text();
	  private int m_first_weight = 1;
	  private int m_second_weight = 1;
	  private final StringBuffer m_buffer = new StringBuffer();
	  
		@Override
	  protected void setup(Context ctx) throws IOException, InterruptedException {
	    super.setup(ctx);
	    Configuration conf = ctx.getConfiguration();
	    m_first_weight = conf.getInt(FIRST_WEIGHT, 1);
	    m_second_weight = conf.getInt(SECOND_WEIGHT, 1);
	  }
		
	   public void reduce(Text key, Iterable<Text> values,  
	           Context context) throws IOException, InterruptedException {            	       
		   String label = "";
		   String titles = "";
		   
		   for (Text val : values) {
	 		  String str = val.toString();
	 		  int pos = str.indexOf(TYPE_SEPARATOR);
	 		  String type = "";
	 		  if (pos == -1)
	 			  continue;
	 		  type = str.substring(0, pos);
	 		  if (type.equals("1")) {
	 			  label = str.substring(pos+1).trim();
	 		  } else if (type.equals("2")) {
	 			  titles = str.substring(pos+1).trim();
	 		  }
	       }
		   
		   if (!label.isEmpty()) {
			   newKey.set("/" + label + "/" + key.toString());
			   m_buffer.delete(0, m_buffer.length());
			   for (int i=0; i<m_first_weight; ++i) {
				   m_buffer.append(key.toString() + " ");
			   }
			   for (int i=0; i<m_second_weight; ++i) {
				   m_buffer.append(titles + " ");
			   }
			   newVal.set(m_buffer.toString());
			   context.write(newKey, newVal);
		   }
	   }     
	
	
}
