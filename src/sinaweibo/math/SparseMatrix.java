package sinaweibo.math;

import com.google.common.collect.AbstractIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Doubly sparse matrix. Implemented as a Map of RandomAccessSparseVector rows */
public class SparseMatrix extends AbstractMatrix {

  private HashMap<Integer, Vector> rowVectors;
  
  /**
   * Construct a matrix of the given cardinality with the given row map
   *
   * @param rows
   *          a Map<Integer, RandomAccessSparseVector> of rows
   * @param columns
   * @param rowVectors
   */
  public SparseMatrix(int rows, int columns, Map<Integer, RandomAccessSparseVector> rowVectors) {
    super(rows, columns);
    this.rowVectors = new HashMap<Integer, Vector> ();
    for (Map.Entry<Integer, RandomAccessSparseVector> entry : rowVectors.entrySet()) {
      this.rowVectors.put(entry.getKey(), entry.getValue().clone());
    }
  }
  
  /**
   * Construct a matrix with specified number of rows and columns.
   */
  public SparseMatrix(int rows, int columns) {
    super(rows, columns);
    this.rowVectors = new HashMap<Integer, Vector> ();
  }

  @Override
  public Matrix clone() {
    SparseMatrix clone = (SparseMatrix) super.clone();
    clone.rowVectors = (HashMap<Integer, Vector>) rowVectors.clone();
    return clone;
  }

  @Override
  public Iterator<MatrixSlice> iterator() {
    final ArrayList<Integer> keys = new ArrayList<Integer>(rowVectors.size());
    for (Integer key: rowVectors.keySet()) {
    	keys.add(key);  	  
    }
    
    return new AbstractIterator<MatrixSlice>() {
      private int slice;
      @Override
      protected MatrixSlice computeNext() {
        if (slice >= rowVectors.size()) {
          return endOfData();
        }
        int i = keys.get(slice);
        Vector row = rowVectors.get(i);
        slice++;
        return new MatrixSlice(row, i);
      }
    };
  }
  
  @Override
  public double getQuick(int row, int column) {
	if (!rowVectors.containsKey(row))
		return 0.0;
    Vector r = rowVectors.get(row);
    return r == null ? 0.0 : r.getQuick(column);
  }
  
  @Override
  public Matrix like() {
    return new SparseMatrix(rowSize(), columnSize());
  }
  
  @Override
  public Matrix like(int rows, int columns) {
    return new SparseMatrix(rows, columns);
  }
  
  @Override
  public void setQuick(int row, int column, double value) {
    Vector r = rowVectors.get(row);
    if (r == null) {
      r = new RandomAccessSparseVector(columnSize());
      rowVectors.put(row, r);
    }
    r.setQuick(column, value);
  }
  
  @Override
  public int[] getNumNondefaultElements() {
    int[] result = new int[2];
    result[ROW] = rowVectors.size();
    for (Vector vectorEntry : rowVectors.values()) {
      result[COL] = Math.max(result[COL], vectorEntry
          .getNumNondefaultElements());
    }
    return result;
  }
  
  @Override
  public Matrix viewPart(int[] offset, int[] size) {
    if (offset[ROW] < 0) {
      throw new IndexException(offset[ROW], rowSize());
    }
    if (offset[ROW] + size[ROW] > rowSize()) {
      throw new IndexException(offset[ROW] + size[ROW], rowSize());
    }
    if (offset[COL] < 0) {
      throw new IndexException(offset[COL], columnSize());
    }
    if (offset[COL] + size[COL] > columnSize()) {
      throw new IndexException(offset[COL] + size[COL], columnSize());
    }
    return new MatrixView(this, offset, size);
  }
  
  @Override
  public Matrix assignColumn(int column, Vector other) {
    if (rowSize() != other.size()) {
      throw new CardinalityException(rowSize(), other.size());
    }
    if (column < 0 || column >= columnSize()) {
      throw new IndexException(column, columnSize());
    }
    for (int row = 0; row < rowSize(); row++) {
      double val = other.getQuick(row);
      if (val != 0.0) {
        Vector r = rowVectors.get(row);
        if (r == null) {
          r = new RandomAccessSparseVector(columnSize());
          rowVectors.put(row, r);
        }
        r.setQuick(column, val);
      }
    }
    return this;
  }
  
  @Override
  public Matrix assignRow(int row, Vector other) {
    if (columnSize() != other.size()) {
      throw new CardinalityException(columnSize(), other.size());
    }
    if (row < 0 || row >= rowSize()) {
      throw new IndexException(row, rowSize());
    }
    rowVectors.put(row, other);
    return this;
  }
  
  @Override
  public Vector viewRow(int row) {
    if (row < 0 || row >= rowSize()) {
      throw new IndexException(row, rowSize());
    }
    Vector res = rowVectors.get(row);
    if (res == null) {
      res = new RandomAccessSparseVector(columnSize());
    }
    return res;
  }
  
}

