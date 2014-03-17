package sinaweibo.app;

import java.io.File;
import weka.classifiers.CheckClassifier;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.evaluation.*;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.*;

public class ClassifyByWeka {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String className = new Object() {
			public String getClassName() {
				String clazzName = this.getClass().getName();
				return clazzName.substring(0, clazzName.lastIndexOf('$'));
			}
		}.getClassName();
		
		if (args.length < 1) {
			System.out.println("Usage: " + className + " <arff-input-name>");
			System.exit(2);
		}
		
		Instances ins = null;
		Classifier cfs = null;
		try {
			// 读入训练测试样本
			File file = new File(args[0]);
			ArffLoader loader = new ArffLoader();
			loader.setFile(file);
			ins = loader.getDataSet();
			ins.setClassIndex(ins.numAttributes() - 1);
			// 初始化分类器
			cfs = (Classifier) Class.forName(
					"weka.classifiers.bayes.NaiveBayes").newInstance();
			// 使用训练样本进行分类
			cfs.buildClassifier(ins);
			// 使用测试样本测试分类器的学习效果
			Instance testInst;
			Evaluation testingEvaluation = new Evaluation(ins);
			int length = ins.numInstances();
			for (int i = 0; i < length; i++) {
				testInst = ins.instance(i);
				testingEvaluation.evaluateModelOnceAndRecordPrediction(cfs,
						testInst);
			}
			// 打印分类结果
			System.out.println("分类的正确率" + (1 - testingEvaluation.errorRate()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
