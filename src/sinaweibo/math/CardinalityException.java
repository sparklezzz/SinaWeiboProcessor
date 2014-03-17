package sinaweibo.math;

public class CardinalityException extends IllegalArgumentException {

  public CardinalityException(int expected, int cardinality) {
    super("Required cardinality " + expected + " but got " + cardinality);
  }

}
