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

package sinaweibo.classifier;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;

import sinaweibo.math.Matrix;
import sinaweibo.math.stats.OnlineSummarizer;

/**
 * ResultAnalyzer captures the classification statistics and displays in a tabular manner
 */
public class ResultAnalyzer {
  
  private final ConfusionMatrix confusionMatrix;
  private final OnlineSummarizer summarizer;
  private boolean hasLL = false;
  
  /*
   * === Summary ===
   * 
   * Correctly Classified Instances 635 92.9722 % Incorrectly Classified Instances 48 7.0278 % Kappa statistic
   * 0.923 Mean absolute error 0.0096 Root mean squared error 0.0817 Relative absolute error 9.9344 % Root
   * relative squared error 37.2742 % Total Number of Instances 683
   */
  private int correctlyClassified;
  
  private int incorrectlyClassified;
  
  public ResultAnalyzer(Collection<String> labelSet, String defaultLabel) {
    confusionMatrix = new ConfusionMatrix(labelSet, defaultLabel);
    summarizer = new OnlineSummarizer();
  }
  
  public ConfusionMatrix getConfusionMatrix() {
    return this.confusionMatrix;
  }
  
  /**
   * 
   * @param correctLabel
   *          The correct label
   * @param classifiedResult
   *          The classified result
   * @return whether the instance was correct or not
   */
  public boolean addInstance(String correctLabel, ClassifierResult classifiedResult) {
    boolean result = correctLabel.equals(classifiedResult.getLabel());
    if (result) {
      correctlyClassified++;
    } else {
      incorrectlyClassified++;
    }
    confusionMatrix.addInstance(correctLabel, classifiedResult);
    if (classifiedResult.getLogLikelihood() != Double.MAX_VALUE) {
      summarizer.add(classifiedResult.getLogLikelihood());
      hasLL = true;
    }
    return result;
  }
  
  /*
   * Added by Xudong Zhang
   * Calculate macro precision, recall and F1
   */
  private double[] calculateMacroMeasurement() {
	double []res = new double[3]; 
	
	Matrix m = confusionMatrix.getMatrix();
	
	// assuming the matrix is a square
	if (m.numCols() != m.numRows()) {
		System.out.println("Confusion matrix is not a square!");
		return null;
	}
	
	
	int numClasses = m.numRows() - 1;	//ignore last default class!
	// num of instances of each class in correct result
	double [] rowSum = new double[numClasses];	
	// num of instances of each class in classified result
	double [] colSum = new double[numClasses];
	//double numInstances = correctlyClassified + incorrectlyClassified;
	for (int i=0; i<numClasses; i++) {
		for (int j=0; j<numClasses; j++) {
			rowSum[i] += m.get(i, j);
			colSum[j] += m.get(i, j);
		}
	}
	
	// calculate precision and recall for each class
	double []precisions = new double[numClasses];
	double []recalls = new double[numClasses];
	double []F1s = new double[numClasses];
	double macroPrecision = 0.0, macroRecall = 0.0, macroF1 = 0.0;
	
	for (int i=0; i<numClasses; ++i) {
		if (Math.abs(colSum[i]) > 1e-6) {
			precisions[i] = m.get(i, i) / colSum[i];
		}
		macroPrecision += precisions[i] / numClasses;
		if (Math.abs(rowSum[i]) > 1e-6) {	
			recalls[i] = m.get(i, i) / rowSum[i];
		}
		macroRecall += recalls[i] / numClasses;
		if (Math.abs(precisions[i]) > 1e-6 && Math.abs(recalls[i]) > 1e-6) {
			F1s[i] = 2 * precisions[i] * recalls[i] / (precisions[i] + recalls[i]);
		}
		macroF1 += F1s[i] / numClasses;
	}
	
	res[0] = macroPrecision;
	res[1] = macroRecall;
	res[2] = macroF1;
	
	return res;
  }
  
  @Override
  public String toString() {
    StringBuilder returnString = new StringBuilder();
    
    returnString.append("=======================================================\n");
    returnString.append("Summary\n");
    returnString.append("-------------------------------------------------------\n");
    int totalClassified = correctlyClassified + incorrectlyClassified;
    double percentageCorrect = (double) 100 * correctlyClassified / totalClassified;
    double percentageIncorrect = (double) 100 * incorrectlyClassified / totalClassified;
    NumberFormat decimalFormatter = new DecimalFormat("0.####");
    
    returnString.append(StringUtils.rightPad("Correctly Classified Instances", 40)).append(": ").append(
      StringUtils.leftPad(Integer.toString(correctlyClassified), 10)).append('\t').append(
      StringUtils.leftPad(decimalFormatter.format(percentageCorrect), 10)).append("%\n");
    returnString.append(StringUtils.rightPad("Incorrectly Classified Instances", 40)).append(": ").append(
      StringUtils.leftPad(Integer.toString(incorrectlyClassified), 10)).append('\t').append(
      StringUtils.leftPad(decimalFormatter.format(percentageIncorrect), 10)).append("%\n");
    returnString.append(StringUtils.rightPad("Total Classified Instances", 40)).append(": ").append(
      StringUtils.leftPad(Integer.toString(totalClassified), 10)).append('\n');
    returnString.append('\n');
    
    double []res = calculateMacroMeasurement();
    if (res != null) {
    	returnString.append(StringUtils.rightPad("Macro Precision", 40)).append(": ").append(
    			StringUtils.leftPad(new Double(res[0] * 100).toString(), 10)).append("%\n");
    	returnString.append(StringUtils.rightPad("Macro Recall", 40)).append(": ").append(
    			StringUtils.leftPad(new Double(res[1] * 100).toString(), 10)).append("%\n");
    	returnString.append(StringUtils.rightPad("Macro F1", 40)).append(": ").append(
    			StringUtils.leftPad(new Double(res[2] * 100).toString(), 10)).append("%\n");
    }
    
    returnString.append(confusionMatrix);
    if (hasLL) {
      returnString.append("\n\n");
      returnString.append("Avg. Log-likelihood: ").append(summarizer.getMean()).append(" 25%-ile: ").append(summarizer.getQuartile(1))
              .append(" 75%-ile: ").append(summarizer.getQuartile(2));
    }

    return returnString.toString();
  }
}
