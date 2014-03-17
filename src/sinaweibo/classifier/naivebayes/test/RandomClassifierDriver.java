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

package sinaweibo.classifier.naivebayes.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import sinaweibo.classifier.ClassifierResult;
import sinaweibo.classifier.ResultAnalyzer;
import sinaweibo.classifier.naivebayes.AbstractNaiveBayesClassifier;
import sinaweibo.classifier.naivebayes.BayesUtils;
import sinaweibo.classifier.naivebayes.ComplementaryNaiveBayesClassifier;
import sinaweibo.classifier.naivebayes.NaiveBayesModel;
import sinaweibo.classifier.naivebayes.StandardNaiveBayesClassifier;
import sinaweibo.common.AbstractJob;
import sinaweibo.common.HadoopUtil;
import sinaweibo.common.Pair;
import sinaweibo.common.commandline.DefaultOptionCreator;
import sinaweibo.common.iterator.sequencefile.PathFilters;
import sinaweibo.common.iterator.sequencefile.PathType;
import sinaweibo.common.iterator.sequencefile.SequenceFileDirIterable;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the (Complementary) Naive Bayes model that was built during training
 * by running the iterating the test set and comparing it to the model
 */
public class RandomClassifierDriver extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(RandomClassifierDriver.class);

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new RandomClassifierDriver(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(addOption(DefaultOptionCreator.overwriteOption().create()));
    addOption("labelIndex", "l", "The path to the location of the label index", true);
    Map<String, List<String>> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      HadoopUtil.delete(getConf(), getOutputPath());
    }
    
    //load the labels
    Map<Integer, String> labelMap = BayesUtils.readLabelIndex(getConf(), new Path(getOption("labelIndex")));

    //loop over the results and create the confusion matrix
    SequenceFileDirIterable<Text, VectorWritable> dirIterable =
        new SequenceFileDirIterable<Text, VectorWritable>(getInputPath(),
                                                          PathType.LIST,
                                                          PathFilters.testFilter(),
                                                          getConf());
    ResultAnalyzer analyzer = new ResultAnalyzer(labelMap.values(), "DEFAULT");
    analyzeResults(labelMap, dirIterable, analyzer);

    log.info("Random Results: {}", analyzer);
    return 0;
  }

  private static void analyzeResults(Map<Integer, String> labelMap,
                                     SequenceFileDirIterable<Text, VectorWritable> dirIterable,
                                     ResultAnalyzer analyzer) {
	  int labelCount = labelMap.size();
	  for (Pair<Text, VectorWritable> pair : dirIterable) {
			int bestIdx = (int) (Math.random() * labelCount);
			double bestScore = Long.MIN_VALUE;
			
			ClassifierResult classifierResult = new ClassifierResult(
					labelMap.get(bestIdx), bestScore);
			analyzer.addInstance(pair.getFirst().toString().split("/")[1],
					classifierResult);
		}
  }
}
