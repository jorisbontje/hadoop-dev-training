package com.cloudera.training.SSSP;

import java.io.*;
import java.lang.*;
import org.apache.hadoop.io.*;

public class Node implements Writable {
  public int[] neighbours;
  public int shortest;

  public Node() {
  }

  public void set(int[] neighbours, int shortest) {
    this.neighbours = neighbours;
    this.shortest = shortest;
  }

  public void set(int[] neighbours) {
    this.neighbours = neighbours;
    this.shortest = 999999;
  }

  public void write(DataOutput out) throws IOException {
    int size = neighbours.length;
    out.writeInt(size);
    for (int i=0; i<size; i++) {
      out.writeInt(neighbours[i]);
    }
    out.writeInt(shortest);
  }

  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    neighbours = new int[size];
    for (int i=0; i<size; i++) {
      neighbours[i] = in.readInt();
    }
    shortest = in.readInt();
  }

  public String toString() {
    String res = "(";
    for (int i=0; i<neighbours.length; i++) {
      if (i!=0) {
        res += ", ";
      }
      res += neighbours[i];
    }
    res += "), "+shortest;
    return(res);
  }
}
