package sinaweibo.classifier.naivebayes;

import java.util.Iterator;

import sinaweibo.math.Vector;
import sinaweibo.math.Vector.Element;

public class PriorNaiveBayesClassifier extends AbstractNaiveBayesClassifier {

	public PriorNaiveBayesClassifier(NaiveBayesModel model) {
		super(model);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected double getScoreForLabelInstance(int label, Vector instance) {		
		double prior = Math.log(model.instanceCount(label) + 1);
		double result = prior;
		boolean empty = true;

		Iterator<Element> elements = instance.iterateNonZero();
		while (elements.hasNext()) {
			empty = false;
			Element e = elements.next();
			result += e.get() * getScoreForLabelFeature(label, e.index());
		}

		if (empty)
			return Long.MIN_VALUE;

		return result;
	}

	@Override
	public double getScoreForLabelFeature(int label, int feature) {
		NaiveBayesModel model = getModel();
		return computeWeight(model.weight(label, feature),
				model.labelWeight(label), model.alphaI(), model.numFeatures());
	}

	public static double computeWeight(double featureLabelWeight,
			double labelWeight, double alphaI, double numFeatures) {
		double numerator = featureLabelWeight + alphaI;
		double denominator = labelWeight + alphaI * numFeatures;
		return Math.log(numerator / denominator);
	}

}
