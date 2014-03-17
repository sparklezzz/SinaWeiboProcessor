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

package sinaweibo.classifier.naivebayes.training;

import com.google.common.base.Splitter;
import com.google.common.io.Closeables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import sinaweibo.classifier.naivebayes.BayesUtils;
import sinaweibo.classifier.naivebayes.NaiveBayesModel;
import sinaweibo.common.AbstractJob;
import sinaweibo.common.HadoopUtil;
import sinaweibo.common.commandline.DefaultOptionCreator;
import sinaweibo.common.iterator.sequencefile.PathFilters;
import sinaweibo.common.iterator.sequencefile.PathType;
import sinaweibo.common.iterator.sequencefile.SequenceFileDirIterable;
import sinaweibo.common.mapreduce.VectorSumReducer;
import sinaweibo.math.DenseVector;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * This class trains a Naive Bayes Classifier (Parameters for both Naive Bayes and Complementary Naive Bayes)
 */
public final class TrainNaiveBayesJob extends AbstractJob {
  private static final String TRAIN_COMPLEMENTARY = "trainComplementary";
  private static final String ALPHA_I = "alphaI";
  private static final String LABEL_INDEX = "labelIndex";
  
  /*
  	Added by Xudong Zhang
  	Store the file of the number of instances in each label
  */
  public static final String LABEL_COUNT = "__LC";
  
  private static final String EXTRACT_LABELS = "extractLabels";
  private static final String LABELS = "labels";
  public static final String WEIGHTS_PER_FEATURE = "__SPF";
  public static final String WEIGHTS_PER_LABEL = "__SPL";
  public static final String LABEL_THETA_NORMALIZER = "_LTN";

  public static final String SUMMED_OBSERVATIONS = "summedObservations";
  public static final String WEIGHTS = "weights";
  public static final String THETAS = "thetas";

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new TrainNaiveBayesJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOutputOption();
    addOption(LABELS, "l", "comma-separated list of labels to include in training", false);

    addOption(buildOption(EXTRACT_LABELS, "el", "Extract the labels from the input", false, false, ""));
    addOption(ALPHA_I, "a", "smoothing parameter", String.valueOf(1.0f));
    addOption(buildOption(TRAIN_COMPLEMENTARY, "c", "train complementary?", false, false, String.valueOf(false)));
    addOption(LABEL_INDEX, "li", "The path to store the label index in", false);
    addOption(DefaultOptionCreator.overwriteOption().create());
    Map<String, List<String>> parsedArgs = parseArguments(args);
    if (parsedArgs == null) {
      return -1;
    }
    if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      HadoopUtil.delete(getConf(), getOutputPath());
      HadoopUtil.delete(getConf(), getTempPath());
    }
    Path labPath;
    String labPathStr = getOption(LABEL_INDEX);
    if (labPathStr != null) {
      labPath = new Path(labPathStr);
    } else {
      labPath = getTempPath(LABEL_INDEX);;
    }
    TreeMap<Integer, Integer> [] instanceCountMap = new TreeMap[1];
    long labelSize = createLabelIndex(labPath, instanceCountMap);
    // write label count map to vectors in disk
    if (instanceCountMap[0] != null) {
 		SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(getConf()), getConf(),
				getTempPath(LABEL_COUNT), Text.class, VectorWritable.class);
 		Vector vec = new DenseVector(instanceCountMap[0].size());
 		for (Entry<Integer, Integer> entry: instanceCountMap[0].entrySet()) {
 			vec.setQuick(entry.getKey(), entry.getValue());
 		}
 		writer.append(new Text(LABEL_COUNT), new VectorWritable(vec));
 		Closeables.closeQuietly(writer);
    }
    
    float alphaI = Float.parseFloat(getOption(ALPHA_I));
    boolean trainComplementary = Boolean.parseBoolean(getOption(TRAIN_COMPLEMENTARY));


    HadoopUtil.setSerializations(getConf());
    HadoopUtil.cacheFiles(labPath, getConf());

    //add up all the vectors with the same labels, while mapping the labels into our index
    Job indexInstances = prepareJob(getInputPath(), getTempPath(SUMMED_OBSERVATIONS), SequenceFileInputFormat.class,
            IndexInstancesMapper.class, IntWritable.class, VectorWritable.class, VectorSumReducer.class, IntWritable.class,
            VectorWritable.class, SequenceFileOutputFormat.class);
    indexInstances.setCombinerClass(VectorSumReducer.class);
    boolean succeeded = indexInstances.waitForCompletion(true);
    if (!succeeded) {
      return -1;
    }
    //sum up all the weights from the previous step, per label and per feature
    Job weightSummer = prepareJob(getTempPath(SUMMED_OBSERVATIONS), getTempPath(WEIGHTS),
            SequenceFileInputFormat.class, WeightsMapper.class, Text.class, VectorWritable.class, VectorSumReducer.class,
            Text.class, VectorWritable.class, SequenceFileOutputFormat.class);
    weightSummer.getConfiguration().set(WeightsMapper.NUM_LABELS, String.valueOf(labelSize));
    weightSummer.setCombinerClass(VectorSumReducer.class);
    succeeded = weightSummer.waitForCompletion(true);
    if (!succeeded) {
      return -1;
    }
    
    //put the per label and per feature vectors into the cache
    HadoopUtil.cacheFiles(getTempPath(WEIGHTS), getConf());
    
    //calculate the Thetas, write out to LABEL_THETA_NORMALIZER vectors -- TODO: add reference here to the part of the Rennie paper that discusses this
    Job thetaSummer = prepareJob(getTempPath(SUMMED_OBSERVATIONS), getTempPath(THETAS),
            SequenceFileInputFormat.class, ThetaMapper.class, Text.class, VectorWritable.class, VectorSumReducer.class,
            Text.class, VectorWritable.class, SequenceFileOutputFormat.class);
    thetaSummer.setCombinerClass(VectorSumReducer.class);
    thetaSummer.getConfiguration().setFloat(ThetaMapper.ALPHA_I, alphaI);
    thetaSummer.getConfiguration().setBoolean(ThetaMapper.TRAIN_COMPLEMENTARY, trainComplementary);

    succeeded = thetaSummer.waitForCompletion(true);
    if (!succeeded) {
      return -1;
    }
    
    //validate our model and then write it out to the official output
    NaiveBayesModel naiveBayesModel = BayesUtils.readModelFromDir(getTempPath(), getConf());
    naiveBayesModel.validate();
    naiveBayesModel.serialize(getOutputPath(), getConf());

    return 0;
  }

  private long createLabelIndex(Path labPath, TreeMap<Integer, Integer> [] instanceCountMap) throws IOException {
    long labelSize = 0;
    if (hasOption(LABELS)) {
      Iterable<String> labels = Splitter.on(",").split(getOption(LABELS));
      labelSize = BayesUtils.writeLabelIndex(getConf(), labels, labPath);
    } else if (hasOption(EXTRACT_LABELS)) {
      SequenceFileDirIterable<Text, IntWritable> iterable =
              new SequenceFileDirIterable<Text, IntWritable>(getInputPath(), PathType.LIST, PathFilters.logsCRCFilter(), getConf());
      labelSize = BayesUtils.writeLabelIndex(getConf(), labPath, iterable, instanceCountMap);
    }
    return labelSize;
  }

}
