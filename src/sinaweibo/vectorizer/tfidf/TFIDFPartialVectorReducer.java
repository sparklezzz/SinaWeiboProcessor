/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sinaweibo.vectorizer.tfidf;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;
import sinaweibo.math.NamedVector;
import sinaweibo.math.RandomAccessSparseVector;
import sinaweibo.math.SequentialAccessSparseVector;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;
import sinaweibo.vectorizer.TFIDF;
import sinaweibo.vectorizer.common.PartialVectorMerger;

/**
 * Converts a document in to a sparse vector
 */
public class TFIDFPartialVectorReducer extends
    Reducer<WritableComparable<?>, VectorWritable, WritableComparable<?>, VectorWritable> {

  private final HashMap<Integer, Long> dictionary = new HashMap<Integer, Long>();

  private final TFIDF tfidf = new TFIDF();

  private int minDf = 1;

  private long maxDf = -1;

  private long vectorCount = 1;

  private long featureCount;

  private boolean sequentialAccess;

  private boolean namedVector;
  
  @Override
  protected void reduce(WritableComparable<?> key, Iterable<VectorWritable> values, Context context)
    throws IOException, InterruptedException {
    Iterator<VectorWritable> it = values.iterator();
    if (!it.hasNext()) {
      return;
    }
    Vector value = it.next().get();
    Iterator<Vector.Element> it1 = value.iterateNonZero();
    Vector vector = new RandomAccessSparseVector((int) featureCount, value.getNumNondefaultElements());
    while (it1.hasNext()) {
      Vector.Element e = it1.next();
      if (!dictionary.containsKey(e.index())) {
        continue;
      }
      long df = dictionary.get(e.index());
      if (maxDf > -1 && (100.0 * df) / vectorCount > maxDf) {
        continue;
      }
      if (df < minDf) {
        df = minDf;
      }
      vector.setQuick(e.index(), tfidf.calculate((int) e.get(), (int) df, (int) featureCount, (int) vectorCount));
    }
    if (sequentialAccess) {
      vector = new SequentialAccessSparseVector(vector);
    }
    
    if (namedVector) {
      vector = new NamedVector(vector, key.toString());
    }
    
    VectorWritable vectorWritable = new VectorWritable(vector);
    context.write(key, vectorWritable);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    URI[] localFiles = DistributedCache.getCacheFiles(conf);
    Preconditions.checkArgument(localFiles != null && localFiles.length >= 1, 
        "missing paths from the DistributedCache");

    vectorCount = conf.getLong(TFIDFConverter.VECTOR_COUNT, 1);
    featureCount = conf.getLong(TFIDFConverter.FEATURE_COUNT, 1);
    minDf = conf.getInt(TFIDFConverter.MIN_DF, 1);
    maxDf = conf.getLong(TFIDFConverter.MAX_DF, -1);
    sequentialAccess = conf.getBoolean(PartialVectorMerger.SEQUENTIAL_ACCESS, false);
    namedVector = conf.getBoolean(PartialVectorMerger.NAMED_VECTOR, false);

    Path dictionaryFile = new Path(localFiles[0].getPath());
    // key is feature, value is the document frequency
    for (Pair<IntWritable,LongWritable> record 
         : new SequenceFileIterable<IntWritable,LongWritable>(dictionaryFile, true, conf)) {
      dictionary.put(record.getFirst().get(), record.getSecond().get());
    }
  }

}
