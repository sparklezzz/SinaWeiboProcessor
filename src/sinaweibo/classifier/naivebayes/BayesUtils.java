package sinaweibo.classifier.naivebayes;

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

import sinaweibo.classifier.naivebayes.training.ThetaMapper;
import sinaweibo.classifier.naivebayes.training.TrainNaiveBayesJob;
import sinaweibo.common.HadoopUtil;
import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.PathFilters;
import sinaweibo.common.iterator.sequencefile.PathType;
import sinaweibo.common.iterator.sequencefile.SequenceFileDirIterable;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;
import sinaweibo.math.Matrix;
import sinaweibo.math.SparseMatrix;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public final class BayesUtils {

	private BayesUtils() {
	}

	public static NaiveBayesModel readModelFromDir(Path base, Configuration conf) {

		float alphaI = conf.getFloat(ThetaMapper.ALPHA_I, 1.0f);

		// read feature sums and label sums
		Vector scoresPerLabel = null;
		Vector scoresPerFeature = null;
		for (Pair<Text, VectorWritable> record : new SequenceFileDirIterable<Text, VectorWritable>(
				new Path(base, TrainNaiveBayesJob.WEIGHTS), PathType.LIST,
				PathFilters.partFilter(), conf)) {
			String key = record.getFirst().toString();
			VectorWritable value = record.getSecond();
			if (key.equals(TrainNaiveBayesJob.WEIGHTS_PER_FEATURE)) {
				scoresPerFeature = value.get();
			} else if (key.equals(TrainNaiveBayesJob.WEIGHTS_PER_LABEL)) {
				scoresPerLabel = value.get();
			}
		}

		Preconditions.checkNotNull(scoresPerFeature);
		Preconditions.checkNotNull(scoresPerLabel);

		Matrix scoresPerLabelAndFeature = new SparseMatrix(
				scoresPerLabel.size(), scoresPerFeature.size());
		for (Pair<IntWritable, VectorWritable> entry : new SequenceFileDirIterable<IntWritable, VectorWritable>(
				new Path(base, TrainNaiveBayesJob.SUMMED_OBSERVATIONS),
				PathType.LIST, PathFilters.partFilter(), conf)) {
			scoresPerLabelAndFeature.assignRow(entry.getFirst().get(), entry
					.getSecond().get());
		}

		Vector perlabelThetaNormalizer = scoresPerLabel.like();
		
		for (Pair<Text,VectorWritable> entry : new
		  SequenceFileDirIterable<Text,VectorWritable>( new Path(base,
		  TrainNaiveBayesJob.THETAS), PathType.LIST, PathFilters.partFilter(),
		  conf)) { 
			if (entry.getFirst().toString().equals(TrainNaiveBayesJob.LABEL_THETA_NORMALIZER)) { 
			  perlabelThetaNormalizer = entry.getSecond().get(); 
			} 
		}
		  
		Preconditions.checkNotNull(perlabelThetaNormalizer);
		
		Vector instanceCountPerLabel = scoresPerLabel.like();
		for (Pair<Text, VectorWritable> record : new SequenceFileIterable<Text, VectorWritable>(
				new Path(base, TrainNaiveBayesJob.LABEL_COUNT), true, conf)) {
			if (record.getFirst().toString().equals(TrainNaiveBayesJob.LABEL_COUNT)) {
				instanceCountPerLabel = record.getSecond().get();
			}
		}
		
		return new NaiveBayesModel(scoresPerLabelAndFeature, scoresPerFeature,
				scoresPerLabel, perlabelThetaNormalizer, instanceCountPerLabel, alphaI);
	}

	/** Write the list of labels into a map file */
	public static int writeLabelIndex(Configuration conf,
			Iterable<String> labels, Path indexPath) throws IOException {
		FileSystem fs = FileSystem.get(indexPath.toUri(), conf);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				indexPath, Text.class, IntWritable.class);
		int i = 0;
		try {
			for (String label : labels) {
				writer.append(new Text(label), new IntWritable(i++));
			}
		} finally {
			Closeables.closeQuietly(writer);
		}
		return i;
	}

	public static int writeLabelIndex(Configuration conf, Path indexPath,
			Iterable<Pair<Text, IntWritable>> labels, TreeMap<Integer, Integer> []countMap) throws IOException {
		FileSystem fs = FileSystem.get(indexPath.toUri(), conf);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				indexPath, Text.class, IntWritable.class);
		Collection<String> seen = new HashSet<String>();
		HashMap<String, Integer> indexMap = new HashMap<String, Integer>();
		countMap[0] = new TreeMap<Integer, Integer>();
		int i = 0;
		try {
			for (Object label : labels) {
				String theLabel = ((Pair<?, ?>) label).getFirst().toString()
						.split("/")[1];
				if (!seen.contains(theLabel)) {
					indexMap.put(theLabel, i);
					countMap[0].put(i, 1);
					
					writer.append(new Text(theLabel), new IntWritable(i++));
					seen.add(theLabel);
				} else {
					Integer idx = indexMap.get(theLabel);
					countMap[0].put(idx, countMap[0].get(idx) + 1);
				}
			}
		} finally {
			Closeables.closeQuietly(writer);
		}
		return i;
	}

	public static Map<Integer, String> readLabelIndex(Configuration conf,
			Path indexPath) {
		Map<Integer, String> labelMap = new HashMap<Integer, String>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(
				indexPath, true, conf)) {
			labelMap.put(pair.getSecond().get(), pair.getFirst().toString());
		}
		return labelMap;
	}

	public static HashMap<String, Integer> readIndexFromCache(
			Configuration conf) throws IOException {
		HashMap<String, Integer> index = new HashMap<String, Integer>();
		for (Pair<Writable, IntWritable> entry : new SequenceFileIterable<Writable, IntWritable>(
				HadoopUtil.cachedFile(conf), conf)) {
			index.put(entry.getFirst().toString(), entry.getSecond().get());
		}
		return index;
	}

	public static Map<String, Vector> readScoresFromCache(Configuration conf)
			throws IOException {
		Map<String, Vector> sumVectors = Maps.newHashMap();
		for (Pair<Text, VectorWritable> entry : new SequenceFileDirIterable<Text, VectorWritable>(
				HadoopUtil.cachedFile(conf), PathType.LIST,
				PathFilters.partFilter(), conf)) {
			sumVectors
					.put(entry.getFirst().toString(), entry.getSecond().get());
		}
		return sumVectors;
	}

}
