package sinaweibo.math.function;

/**
 * Interface that represents a function object: a function that takes a single argument and returns a single value.
 * @see org.apache.mahout.math.map
 */
public interface DoubleFunction {

  /**
   * Apply the function to the argument and return the result
   *
   * @param arg1 double for the argument
   * @return the result of applying the function
   */
  double apply(double arg1);

}
