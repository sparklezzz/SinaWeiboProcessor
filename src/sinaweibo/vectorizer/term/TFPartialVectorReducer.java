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

package sinaweibo.vectorizer.term;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import sinaweibo.common.Pair;
import sinaweibo.common.StringTuple;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;
import sinaweibo.common.lucene.IteratorTokenStream;
import sinaweibo.math.NamedVector;
import sinaweibo.math.RandomAccessSparseVector;
import sinaweibo.math.SequentialAccessSparseVector;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;
import sinaweibo.vectorizer.DictionaryVectorizer;
import sinaweibo.vectorizer.common.PartialVectorMerger;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Converts a document in to a sparse vector
 */
public class TFPartialVectorReducer extends Reducer<Text, StringTuple, Text, VectorWritable> {

  private final HashMap<String, Integer> dictionary = new HashMap<String, Integer>();

  private int dimension;

  private boolean sequentialAccess;

  private boolean namedVector;

  private int maxNGramSize = 1;

  @Override
  protected void reduce(Text key, Iterable<StringTuple> values, Context context)
          throws IOException, InterruptedException {
    Iterator<StringTuple> it = values.iterator();
    if (!it.hasNext()) {
      return;
    }
    StringTuple value = it.next();

    Vector vector = new RandomAccessSparseVector(dimension, value.length()); // guess at initial size

    if (maxNGramSize >= 2) {
      ShingleFilter sf = new ShingleFilter(new IteratorTokenStream(value.getEntries().iterator()), maxNGramSize);
      try {
        do {
          String term = sf.getAttribute(CharTermAttribute.class).toString();
          if (!term.isEmpty() && dictionary.containsKey(term)) { // ngram
            int termId = dictionary.get(term);
            vector.setQuick(termId, vector.getQuick(termId) + 1);
          }
        } while (sf.incrementToken());

        sf.end();
      } finally {
        Closeables.closeQuietly(sf);
      }
    } else {
      for (String term : value.getEntries()) {
        if (!term.isEmpty() && dictionary.containsKey(term)) { // unigram
          int termId = dictionary.get(term);
          vector.setQuick(termId, vector.getQuick(termId) + 1);
        }
      }
    }
    if (sequentialAccess) {
      vector = new SequentialAccessSparseVector(vector);
    }

    if (namedVector) {
      vector = new NamedVector(vector, key.toString());
    }

    // if the vector has no nonZero entries (nothing in the dictionary), let's not waste space sending it to disk.
    if (vector.getNumNondefaultElements() > 0) {
      VectorWritable vectorWritable = new VectorWritable(vector);
      context.write(key, vectorWritable);
    } else {
      context.getCounter("TFParticalVectorReducer", "emptyVectorCount").increment(1);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    URI[] localFiles = DistributedCache.getCacheFiles(conf);
    Preconditions.checkArgument(localFiles != null && localFiles.length >= 1,
            "missing paths from the DistributedCache");

    dimension = conf.getInt(PartialVectorMerger.DIMENSION, Integer.MAX_VALUE);
    sequentialAccess = conf.getBoolean(PartialVectorMerger.SEQUENTIAL_ACCESS, false);
    namedVector = conf.getBoolean(PartialVectorMerger.NAMED_VECTOR, false);
    maxNGramSize = conf.getInt(DictionaryVectorizer.MAX_NGRAMS, maxNGramSize);

    Path dictionaryFile = new Path(localFiles[0].getPath());
    // key is word value is id
    for (Pair<Writable, IntWritable> record
            : new SequenceFileIterable<Writable, IntWritable>(dictionaryFile, true, conf)) {
      dictionary.put(record.getFirst().toString(), record.getSecond().get());
    }
  }

}
