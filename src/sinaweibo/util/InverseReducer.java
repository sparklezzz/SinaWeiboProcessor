package sinaweibo.util;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class InverseReducer<K, V> extends Reducer<K, V, V, K> {
	  /** The inverse function.  Input keys and values are swapped.*/
	  @Override
	  public void reduce(K key, Iterable<V> values, Context context
	                  ) throws IOException, InterruptedException {
	    for (V value : values) {
		  context.write(value, key);
	    }
	  }
}
