package sinaweibo.math;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;


/** Implements vector that only stores non-zero doubles */
public class RandomAccessSparseVector extends AbstractVector {

  private static final int INITIAL_CAPACITY = 11;

  //private OpenIntDoubleHashMap values;
  private HashMap<Integer, Double> values;
  
  
  /** For serialization purposes only. */
  public RandomAccessSparseVector() {
    super(0);
  }

  public RandomAccessSparseVector(int cardinality) {
    this(cardinality, Math.min(cardinality, INITIAL_CAPACITY)); // arbitrary estimate of 'sparseness'
  }

  public RandomAccessSparseVector(int cardinality, int initialCapacity) {
    super(cardinality);
    values = new HashMap<Integer, Double>(initialCapacity);
    
    //values = new OpenIntDoubleHashMap(initialCapacity);
  }

  public RandomAccessSparseVector(Vector other) {
    this(other.size(), other.getNumNondefaultElements());
    Iterator<Element> it = other.iterateNonZero();
    Element e;
    while (it.hasNext() && (e = it.next()) != null) {
      values.put(e.index(), e.get());
    }
  }

  private RandomAccessSparseVector(int cardinality, HashMap<Integer, Double> values) {
    super(cardinality);
    this.values = values;
  }

  public RandomAccessSparseVector(RandomAccessSparseVector other, boolean shallowCopy) {
    super(other.size());
    values = shallowCopy ? other.values : (HashMap<Integer, Double>)other.values.clone();
  }

  @Override
  public RandomAccessSparseVector clone() {
    return new RandomAccessSparseVector(size(), (HashMap<Integer, Double>) values.clone());
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append('{');
    Iterator<Element> it = iterateNonZero();
    boolean first = true;
    while (it.hasNext()) {
      if (first) {
        first = false;
      } else {
        result.append(',');
      }
      Element e = it.next();
      result.append(e.index());
      result.append(':');
      result.append(e.get());
    }
    result.append('}');
    return result.toString();
  }

  @Override
  public Vector assign(Vector other) {
    if (size() != other.size()) {
      throw new CardinalityException(size(), other.size());
    }
    values.clear();
    Iterator<Element> it = other.iterateNonZero();
    Element e;
    while (it.hasNext() && (e = it.next()) != null) {
      setQuick(e.index(), e.get());
    }
    return this;
  }

  /**
   * @return false
   */
  @Override
  public boolean isDense() {
    return false;
  }

  /**
   * @return false
   */
  @Override
  public boolean isSequentialAccess() {
    return false;
  }

  @Override
  public double getQuick(int index) {
	if (!values.containsKey(index))
		return 0.0;
    return values.get(index);
  }

  @Override
  public void setQuick(int index, double value) {
    lengthSquared = -1.0;
    if (value == 0.0) {
      values.remove(index);
    } else {
      values.put(index, value);
    }
  }

  @Override
  public int getNumNondefaultElements() {
    return values.size();
  }

  @Override
  public RandomAccessSparseVector like() {
    return new RandomAccessSparseVector(size(), values.size());
  }

  /**
   * NOTE: this implementation reuses the Vector.Element instance for each call of next(). If you need to preserve the
   * instance, you need to make a copy of it
   *
   * @return an {@link Iterator} over the Elements.
   * @see #getElement(int)
   */
  @Override
  public Iterator<Element> iterateNonZero() {
    return new NonDefaultIterator();
  }
  
  @Override
  public Iterator<Element> iterator() {
    return new AllIterator();
  }

  private final class NonDefaultIterator extends AbstractIterator<Element> {

    private final RandomAccessElement element = new RandomAccessElement();
    private final ArrayList<Integer> indices = new ArrayList<Integer>();
    private int offset;

    private NonDefaultIterator() {
      indices.clear();
      for (Integer key: values.keySet()) {
    	  indices.add(key);  	  
      }
    }

    @Override
    protected Element computeNext() {
      if (offset >= indices.size()) {
        return endOfData();
      }
      element.index = indices.get(offset);
      offset++;
      return element;
    }

  }

  private final class AllIterator extends AbstractIterator<Element> {

    private final RandomAccessElement element = new RandomAccessElement();

    private AllIterator() {
      element.index = -1;
    }

    @Override
    protected Element computeNext() {
      if (element.index + 1 < size()) {
        element.index++;
        return element;
      } else {
        return endOfData();
      }
    }

  }

  private final class RandomAccessElement implements Element {

    int index;

    @Override
    public double get() {
      if (!values.containsKey(index))
    	  return 0;
      return values.get(index);
    }

    @Override
    public int index() {
      return index;
    }

    @Override
    public void set(double value) {
      lengthSquared = -1;
      if (value == 0.0) {
        values.remove(index);
      } else {
        values.put(index, value);
      }
    }
  }
  
  @Override
  protected Matrix matrixLike(int rows, int columns) {
    return new SparseRowMatrix(rows, columns);
  }  
  
}

