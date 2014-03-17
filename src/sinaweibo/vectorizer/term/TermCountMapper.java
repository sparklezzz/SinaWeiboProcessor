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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import sinaweibo.common.StringTuple;
import sinaweibo.vectorizer.DictionaryVectorizer;


/**
 * TextVectorizer Term Count Mapper. Tokenizes a text document and outputs the count of the words
 */
public class TermCountMapper extends Mapper<Text, StringTuple, Text, LongWritable> {
	
  @Override
  protected void map(Text key, StringTuple value, final Context context) throws IOException, InterruptedException {
    HashMap<String, Long> wordCount = new HashMap<String, Long>();
    for (String word : value.getEntries()) {
      if (wordCount.containsKey(word)) {
        wordCount.put(word, wordCount.get(word) + 1);
      } else {
        wordCount.put(word, (long) 1);
      }
    }
    
    for (Entry<String, Long> entry: wordCount.entrySet()) {
    	try {
          context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
        } catch (IOException e) {
          context.getCounter("Exception", "Output IO Exception").increment(1);
        } catch (InterruptedException e) {
          context.getCounter("Exception", "Interrupted Exception").increment(1);
        }
    }
  }
}
