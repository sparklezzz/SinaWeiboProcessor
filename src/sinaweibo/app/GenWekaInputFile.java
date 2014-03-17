package sinaweibo.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONObject;

import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;

import sinaweibo.common.Pair;
import sinaweibo.common.iterator.sequencefile.SequenceFileIterable;
import sinaweibo.math.Vector;
import sinaweibo.math.VectorWritable;
import sinaweibo.math.Vector.Element;
import sinaweibo.vectorizer.DictionaryVectorizer;
import sinaweibo.vectorizer.tfidf.TFIDFConverter;


/*
 * Description: Generate .arff file for weka
 * Author: Xudong Zhang
 * Create time: 2013/12/06
 */

public class GenWekaInputFile {
	
	public static HashMap<Integer, Integer> m_oldID2NewIDMap = new HashMap<Integer, Integer>();
	public static TreeMap<Integer, String> m_newID2WordMap = new TreeMap<Integer, String>();
	
	/*
	// load map of oldid -> newid
	public static void loadOldID2NewIDMap(Configuration conf, Path remapPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);		
		for (Pair<IntWritable, IntWritable> record : new SequenceFileIterable<IntWritable, IntWritable>(
				remapPath, true, conf)) {
			m_oldID2NewIDMap.put(record.getFirst().get(), record.getSecond().get());
		}
	}
	*/
	
	// load map of newid -> word, sorted by newid
	public static void loadNewID2WordMap(Configuration conf, Path wordDictDirPath) throws Exception {
		System.out.println("Generating new id to word map...");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(wordDictDirPath,
				DictionaryVectorizer.DICTIONARY_FILE + "*"));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		ArrayList<String> pathArr = new ArrayList<String>();
		for (Path p: listedPaths) {
			pathArr.add(p.toString());
		}
		Collections.sort(pathArr);
		int index = 0;
		for (String ps: pathArr) {
			Path p = new Path(ps);
			for (Pair<Text, IntWritable> record : new SequenceFileIterable<Text, IntWritable>(
					p, true, conf)) {
				
				if (!m_oldID2NewIDMap.containsKey(record.getSecond().get()))
					continue;
				m_newID2WordMap.put(m_oldID2NewIDMap.get(record.getSecond().get()), 
						record.getFirst().toString());

				index ++;
				if (index % 10000 == 0) {
					System.out.println("Having wrote " + index + " words!");
				}
			}
		}
	}
	
	// gen old id 2 new ip map from existing vectors
	// only choose old ids which have top DF
	public static void genOldID2NewIDMap(Configuration conf, Path vectorDirPath, float topRatio) throws IOException {
		System.out.println("transforming old id to new id...");
		HashMap<Integer, Integer> oldID2DFMap = Maps.newHashMap();
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(vectorDirPath.toString(), "*"));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		int count = 0;
		for (Path p: listedPaths) {
			if (p.getName().toString().startsWith("_"))
				continue;
			System.out.println("Opening " + p.toString() + " ...");
			for (Pair<Text, VectorWritable> record : new SequenceFileIterable<Text, VectorWritable>(
					p, true, conf)) {
				
				Vector vector = record.getSecond().get();
				Iterator<Element> iter = vector.iterateNonZero();
				while (iter.hasNext()) {
					Element e = iter.next();
					
					if (!oldID2DFMap.containsKey(e.index())) {
						oldID2DFMap.put(e.index(), 1);
					} else {	//TODO: we only account for DF here, not TF-IDF 
						oldID2DFMap.put(e.index(), oldID2DFMap.get(e.index()) + 1);
					}
				}								
				count ++;
				if (count % 10000 == 0) {
					System.out.println("Having wrote " + count + " vectors!");
				}
			}
		}
		
		// reverse the DF count map, use DF as key, and keep order
		// note there may be multiple word ids which have same DF!
		/*TreeMultimap<Integer, Integer> DF2OldIDMutliMap = TreeMultimap.create();
		for (Entry<Integer, Integer> entry : oldID2DFMap.entrySet()) {
			DF2OldIDMutliMap.put(entry.getValue(), entry.getKey());
		}
		// do not use DF2OldIDMutliMap.keySet().size();
		int threshold = (int)((float)DF2OldIDMutliMap.size() * topRatio);
		System.out.println("top words reserved: " + threshold);
		
		// create old id to new id map
		SortedMap<Integer, Collection<Integer>> tmpMap = DF2OldIDMutliMap.asMap().;
		for (Entry<Integer, Collection<Integer>> entry : tmpMap.entrySet()) {
			
		}*/
		int totalSize = 0;
		TreeMap<Integer, ArrayList<Integer> > DF2OldIDMultiMap = new TreeMap(Collections.reverseOrder());
		for (Entry<Integer, Integer> entry : oldID2DFMap.entrySet()) {
			Integer oldID = entry.getKey();
			Integer df = entry.getValue();
			if (!DF2OldIDMultiMap.containsKey(df)) {
				DF2OldIDMultiMap.put(df, new ArrayList<Integer>());
			}
			DF2OldIDMultiMap.get(df).add(oldID);
			totalSize ++;
		}
		int threshold = (int)((float)totalSize * topRatio);
		System.out.println("Total words count: " + totalSize + ", top words reserved: " + threshold);
		
		// create old id to new id map
		count = 0;
		for (Entry<Integer, ArrayList<Integer>> entry : DF2OldIDMultiMap.entrySet()) {
			ArrayList<Integer> tmpArr = entry.getValue();
			boolean endFlag = false;
			for (Integer oldID: tmpArr) {
				m_oldID2NewIDMap.put(oldID, count);
				count ++;
				if (count > threshold) {
					endFlag = true;
					break;
				}
			}
			if (endFlag)
				break;
		}
	}
	
	// write attributes and return the index of class attribute
	public static int writeAttributes(Configuration conf, String property,  
			Path propDictPath, BufferedWriter bw) throws Exception {
		if (null == bw)	
			return -1;
		FileSystem fs = FileSystem.get(conf);
		
		// write word dict
		int index = 0;
		for (Entry<Integer, String> entry : m_newID2WordMap.entrySet()) {
			if (index != entry.getKey()) {
				throw(new Exception("Unmatched index: " + index + " " + entry.getKey()));
			}
			
			bw.write("@attribute \"" + entry.getValue() + "\"\treal\n");
			index ++;
		}
		
		//write label attribute		
		FSDataInputStream fsis = fs.open(propDictPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fsis, "UTF8"));
		String line;
		while ((line = br.readLine()) != null) {
			JSONObject jsonObj = new JSONObject(line.toString());
			if (!jsonObj.has("title") || !jsonObj.getString("title").equals(property)) {
				continue;
			}
			bw.write("@attribute class\t");
			boolean first = true;
			Iterator<?> valIter = jsonObj.keys();
			while (valIter.hasNext()) {	
				String val = (String)valIter.next();
				if (val.equals("0") || val.equals("title"))
					continue;
				if (first) {
					bw.write("{");
					first = false;
				} else {
					bw.write(",");
				}
				bw.write(val);
			}
			bw.write("}\n");
			break;
		}
		br.close();
		
		return index;
	}
	
	public static void writeData(Configuration conf, Path vectorDirPath, int labelIndex, 
			BufferedWriter bwTrain, BufferedWriter bwTest) throws IOException {
		System.out.println("Writing vectors...");
		bwTrain.write("@data\n");
		bwTest.write("@data\n");
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(vectorDirPath.toString(), "*"));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		TreeMap<Integer, Double> tmpVec = new TreeMap<Integer, Double>();	// use treemap to keep order during iteration
		int count = 0;
		for (Path p: listedPaths) {
			if (p.getName().toString().startsWith("_"))
				continue;
			System.out.println("Opening " + p.toString() + " ...");
			for (Pair<Text, VectorWritable> record : new SequenceFileIterable<Text, VectorWritable>(
					p, true, conf)) {
				tmpVec.clear();
				
				Vector vector = record.getSecond().get();
				Iterator<Element> iter = vector.iterateNonZero();
				while (iter.hasNext()) {
					Element e = iter.next();
					
					// filter and transfrom old id to new id
					if (!m_oldID2NewIDMap.containsKey(e.index()))
						continue;
					
					tmpVec.put(m_oldID2NewIDMap.get(e.index()), e.get());
				}
				
				if (tmpVec.size() == 0)
					continue;
				
				String label = record.getFirst().toString().split("/")[1];
				boolean first = true;
				if (label != null && !label.equals("null") && !label.equals("0") 
						&& !label.equals("-") && !label.isEmpty()) {
					// write each dimension
					for (Entry<Integer, Double> entry : tmpVec.entrySet()) {
						if (first) {
							bwTrain.write("{");
							first = false;
						} else {
							bwTrain.write(", ");
						}
						bwTrain.write(entry.getKey().toString() + " " + entry.getValue().toString());
					}
					bwTrain.write(", " + labelIndex + " " + label + "}\n");	
				} else {
					// write each dimension
					for (Entry<Integer, Double> entry : tmpVec.entrySet()) {
						if (first) {
							bwTest.write("{");
							first = false;
						} else {
							bwTest.write(", ");
						}
						bwTest.write(entry.getKey().toString() + " " + entry.getValue().toString());
					}
					bwTest.write("}\n");
				}
										
				count ++;
				if (count % 10000 == 0) {
					System.out.println("Having wrote " + count + " vectors!");
				}
			}
		}
	}
	
	
	public static void main(String []args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();

		if (otherArgs.length < 4) {
			System.err.println("Usage: " + className
					+ " <prop-definition-path> <top-words-keep-ratio> <input-dir> <local-outdir>");
			System.exit(2);
		}

		String propDefPathStr = otherArgs[0];
		float topWordsRatio = Float.parseFloat(otherArgs[1]);
		String inputDirStr = otherArgs[2];
		String localOutDirStr = otherArgs[3];
		
		File localOutDir = new File(localOutDirStr);
		localOutDir.mkdirs();
		if (!localOutDir.isDirectory()) {
			throw new Exception("Cannot mkdir: " + localOutDirStr);
		}
		
		//loadOldID2NewIDMap(conf, new Path(inputDirStr, TFIDFConverter.DICT_REMAP_FILE));
		genOldID2NewIDMap(conf, new Path(inputDirStr, TFIDFConverter.DOCUMENT_VECTOR_OUTPUT_FOLDER), topWordsRatio);
		loadNewID2WordMap(conf, new Path(inputDirStr));
		
		for (String property: GlobalName.PROFILE_PROPERTY_SET) {
			// write train and test data
			String localTrainFilePathStr = localOutDirStr + "/" + property + "_train.arff";
			String localTestFilePathStr = localOutDirStr + "/" + property + "_test.arff";
			System.out.println("Writing to local train file " + localTrainFilePathStr + " ...");
			System.out.println("Writing to local test file " + localTestFilePathStr + " ...");
			BufferedWriter bwTrain = new BufferedWriter(new FileWriter(
					localTrainFilePathStr));
			BufferedWriter bwTest = new BufferedWriter(new FileWriter(
					localTestFilePathStr));
			
			bwTrain.write("@relation " + property + "\n\n");
			bwTest.write("@relation " + property + "\n\n");
			
			int labelIndex = writeAttributes(conf, property, new Path(propDefPathStr), bwTrain);
			writeAttributes(conf, property, new Path(propDefPathStr), bwTest);
			bwTrain.write("\n");
			bwTest.write("\n");
			
			writeData(conf, new Path(inputDirStr, GlobalName.WEIBO_LABELED_VECTOR_DIR_PREFIX + property), 
					labelIndex, bwTrain, bwTest);
			bwTrain.close();
			bwTest.close();
			
			
		}
	}
}
