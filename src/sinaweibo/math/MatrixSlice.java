package sinaweibo.math;

import sinaweibo.math.Vector;

public class MatrixSlice {

  private final Vector v;
  private final int index;

  public MatrixSlice(Vector v, int index) {
    this.v = v;
    this.index = index;
  }

  public Vector vector() { return v; }
  public int index() { return index; }
}
