package sinaweibo.math.function;

/**
 * Interface that represents a function object: a function that takes two arguments and returns a single value.
 **/
public interface DoubleDoubleFunction {

  /**
   * Apply the function to the arguments and return the result
   *
   * @param arg1 a double for the first argument
   * @param arg2 a double for the second argument
   * @return the result of applying the function
   */
  double apply(double arg1, double arg2);
}
