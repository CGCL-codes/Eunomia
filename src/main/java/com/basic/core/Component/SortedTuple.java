package com.basic.core.Component;

import org.apache.storm.tuple.Tuple;

public class SortedTuple {
  private Tuple tuple;

  public SortedTuple(Tuple tuple) {
    this.tuple = tuple;
  }

  public Tuple getTuple() {
    return tuple;
  }

}
