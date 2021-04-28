package com.basic.core.Index;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.storm.tuple.Values;
import static com.basic.core.Utils.CastUtils.getLong;

public class SubIndex {
  private Multimap<Object, Values> tuples;
  private long minTimeStamp;
  private long maxTimeStamp;

  public SubIndex() {
    tuples = HashMultimap.create();
    minTimeStamp = Long.MAX_VALUE;
    maxTimeStamp = -1;
  }

  public void add(Values tuple) {
    tuples.put(tuple.get(2), tuple);
    long ts = getLong(tuple.get(1));
    minTimeStamp = Math.min(ts, minTimeStamp);
    maxTimeStamp = Math.max(ts, maxTimeStamp);
  }

  public void clear() {
    tuples.clear();
    minTimeStamp = Long.MAX_VALUE;
    maxTimeStamp = -1;

  }

  public Collection<Values> getTuplesByKey(Object key) {
    return tuples.get(key);
  }

  public long getMinTimeStamp() {
    return minTimeStamp;
  }

  public long getMaxTimeStamp() {
    return maxTimeStamp;
  }

  public int getSize() {
    return tuples.size();
  }

  public Collection<Values> getTuples() {
    return tuples.values();
  }

}
