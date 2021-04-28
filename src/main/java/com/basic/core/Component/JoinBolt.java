package com.basic.core.Component;

import static com.basic.core.Utils.CastUtils.getBoolean;
import static com.basic.core.Utils.CastUtils.getInt;
import static com.basic.core.Utils.CastUtils.getLong;
import static com.basic.core.Utils.CastUtils.getString;
import static com.basic.core.Utils.Config.ACK_TO_DISPATCHER_STREAM_ID;
import static com.basic.core.Utils.Config.DISPATCHER_BOLT_ID;
import static com.basic.core.Utils.Config.FORWARD_TUPLE_STREAM_ID;
import static com.basic.core.Utils.Config.JOINER_BOLT_ID;
import static com.basic.core.Utils.Config.JOIN_RESULTS_STREAM_ID;
import static com.basic.core.Utils.Config.METRIC_STREAM_ID;
import static com.basic.core.Utils.Config.ROUTE_TABLE_JOINER_STREAM_ID;
import static com.basic.core.Utils.Config.SCHEMA;
import static com.basic.core.Utils.Config.SHUFFLE_R_STREAM_ID;
import static org.slf4j.LoggerFactory.getLogger;

import com.basic.core.Index.SubIndex;
import com.basic.core.Utils.StopWatch;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;

public class JoinBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(JoinBolt.class);
  private static final List<String> METRIC_SCHEMA = ImmutableList.of("currentMoment", "tuples", "results",
    "processingDuration", "latency");

  private int taskId;
  private boolean begin;
  private long numProcessed;
  private long numLastProcessed;
  private long numResults;
  private long numLastResults;
  private long lastOutputTime;
  private double latency;

  private int subIndexSize;
  private SubIndex activeSubIndexR;
  private SubIndex activeSubIndexS;
  private Queue<SubIndex> indexChainR;
  private Queue<SubIndex> indexChainS;

  private boolean isWindowJoin;
  private long windowLength;

  private StopWatch stopWatch;
  private long profileReportInSeconds;
  private long triggerReportInSeconds;

  private boolean checkDuplicate;

  private Queue<SortedTuple> bufferedTuples;
  private Map<Integer, Long> upstreamBarriers;
  private long numUpstreamTask;
  private long barrierNum = 0l;
  private int root;
  private int degree;
  private List<Integer> joinerTasks;
  private HashMap<Integer, Integer> joinerIndex;
  private List<Integer> forwardTasks;
//  private Map<String, Integer> joinedResults;
//  private Map<String, String> getTuples;
//  private int round = 0;

  public JoinBolt(boolean cd, int r, int d) {
    super();
    checkDuplicate = cd;
    root = r;
    degree = d;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    taskId = context.getThisTaskId();
    numProcessed = 0l;
    numResults = 0l;
    numLastResults = 0l;
    subIndexSize = getInt(stormConf.get("SUB_INDEX_SIZE"));
    isWindowJoin = getBoolean(stormConf.get("WINDOW_ENABLE"));
    windowLength = getLong(stormConf.get("WINDOW_LENGTH"));
    activeSubIndexR = new SubIndex();
    activeSubIndexS = new SubIndex();
    indexChainR = new PriorityQueue<>(Comparator.comparingLong(SubIndex::getMaxTimeStamp));
    indexChainS = new PriorityQueue<>(Comparator.comparingLong(SubIndex::getMaxTimeStamp));
    begin = true;
    stopWatch = StopWatch.createStarted();
    profileReportInSeconds = 1;
    triggerReportInSeconds = 1;
//    joinedResults = new HashMap();
//    getTuples = new HashMap<>();
    numUpstreamTask = context.getComponentTasks(DISPATCHER_BOLT_ID).size();

    bufferedTuples = new PriorityQueue<>((o1, o2) -> {
      Long b1 = o1.getTuple().getLongByField("barrier");
      Long b2 = o2.getTuple().getLongByField("barrier");
      if (b1 == b2) {
        return o1.getTuple().getLongByField("timestamp").compareTo(o2.getTuple().getLongByField("timestamp"));
      } else {
        return b1.compareTo(b2);
      }
    });
    upstreamBarriers = new HashMap<>();

    forwardTasks = new ArrayList<>();

    joinerTasks = context.getComponentTasks(JOINER_BOLT_ID);
    int num = joinerTasks.size();
    joinerIndex = new HashMap<>();
    int [][] router = new int[num][degree];
    int numJoinersTree = (int) Math.ceil((double) num / root);
    int [][] forest = new int[root][numJoinersTree];

    for (int i = 0; i < root; i++) {
      for (int j = 0; j < numJoinersTree; j++) {
        forest[i][j] = -1;
      }
    }

    for (int i = 0; i < joinerTasks.size(); i++) {
      joinerIndex.put(joinerTasks.get(i), i);
      for (int j = 0; j < degree; j++) {
        router[i][j] = -1;
      }
      int n = i / root;
      int m = i % root;
      forest[m][n] = i;
    }



    for (int i = 0; i < forest.length; i++) {
      boolean flag = false;
      for (int j = 0; j < numJoinersTree; j++) {
        for (int d = 1; d<= degree; d++) {
          int child = j * degree + d;
          if (child >= numJoinersTree || forest[i][child] == -1) {
            flag = true;
            break;
          }
          router[forest[i][j]][d - 1] = joinerTasks.get(forest[i][child]);
        }
        if (flag) {
          break;
        }
      }
    }

    int index = joinerIndex.get(taskId);
    for (int i = 0; i < degree; i++) {
      if (router[index][i] != -1) {
        forwardTasks.add(router[index][i]);
      }
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    if (tuple.getSourceStreamId().equals(ROUTE_TABLE_JOINER_STREAM_ID)) {
      basicOutputCollector.emit(ACK_TO_DISPATCHER_STREAM_ID, new Values("ack"));
      root = tuple.getIntegerByField("root");
      degree = tuple.getIntegerByField("degree");

      List<List<Integer>> router = (List<List<Integer>>) tuple.getValueByField("router");
      for (int targetTask : forwardTasks) {
        Values values = new Values(root, degree, router);
        basicOutputCollector.emitDirect(targetTask, ROUTE_TABLE_JOINER_STREAM_ID, values);
      }

      forwardTasks.clear();
      int index = joinerIndex.get(taskId);
      for (int i = 0; i < degree; i++) {
        if (router.get(index).get(i) != -1) {
          forwardTasks.add(router.get(index).get(i));
        }
      }
    } else {
//      long cur = stopWatch.elapsed(TimeUnit.MICROSECONDS);
        executeTuple(tuple, basicOutputCollector);
//      if (root > 1 && tuple.getSourceComponent().equals(DISPATCHER_BOLT_ID)) {
//        int upstreamTask = tuple.getSourceTask();
//        long barrier = tuple.getLongByField("barrier");
//        bufferedTuples.offer(new SortedTuple(tuple));
//        if (!upstreamBarriers.containsKey(upstreamTask) || barrier > upstreamBarriers.get(upstreamTask)) {
//          upstreamBarriers.put(upstreamTask, barrier);
//          long tempBarrier = checkBarrier();
//          if (tempBarrier > barrierNum) {
//            barrier = tempBarrier;
//            executeBufferedTuples(barrier, basicOutputCollector);
//          }
//        }
//      } else {
//        executeTuple(tuple, basicOutputCollector);
//      }

//      latency += stopWatch.elapsed(TimeUnit.MICROSECONDS) - cur;
    }

    if (isTimeToOutputProfile()) {
      long moment = stopWatch.elapsed(TimeUnit.SECONDS);
      long tuples = numProcessed - numLastProcessed;
      long processingDuration = stopWatch.elapsed(TimeUnit.MILLISECONDS) - lastOutputTime;
      long results = numResults - numLastResults;
      basicOutputCollector.emit(METRIC_STREAM_ID, new Values(moment, tuples, results, processingDuration, latency));
      numLastProcessed = numProcessed;
      numLastResults = numResults;
      lastOutputTime = stopWatch.elapsed(TimeUnit.MILLISECONDS);
      latency = 0;
    }
  }

  public void executeTuple(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    numProcessed++;
    forward(tuple, basicOutputCollector);
    if (tuple.getIntegerByField("target") == this.taskId) {
      store(tuple);
    }
    join(tuple, basicOutputCollector);

  }

  public void executeBufferedTuples(Long barrier, BasicOutputCollector basicOutputCollector) {
    while (!bufferedTuples.isEmpty()) {
      SortedTuple tempTuple = bufferedTuples.peek();
      if (tempTuple.getTuple().getLongByField("barrier") <= barrier) {
        executeTuple(tempTuple.getTuple(), basicOutputCollector);
//        ret = Math.min(ret, tempTuple.getTimeStamp());
        bufferedTuples.poll();
      } else {
        break;
      }
    }
  }

  private Long checkBarrier() {
    if (upstreamBarriers.size() != numUpstreamTask) {
      return barrierNum;
    }
    long tempBarrier = Long.MAX_VALUE;
    for (Map.Entry<Integer, Long> entry : upstreamBarriers.entrySet()) {
      tempBarrier = Math.min(entry.getValue(), tempBarrier);
    }
    return tempBarrier;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(JOIN_RESULTS_STREAM_ID, new Fields("value", "rel"));
    declarer.declareStream(METRIC_STREAM_ID, new Fields(METRIC_SCHEMA));
    declarer.declareStream(FORWARD_TUPLE_STREAM_ID, new Fields(SCHEMA));
    declarer.declareStream(ACK_TO_DISPATCHER_STREAM_ID, new Fields("relation"));
    declarer.declareStream(ROUTE_TABLE_JOINER_STREAM_ID, new Fields("root", "degree", "router"));

  }

  public void forward(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    Integer target = tuple.getIntegerByField("target");
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    String key = tuple.getStringByField("key");
    String value = tuple.getStringByField("value");

    Values values = new Values(target, rel, ts, key, value);

    for (int targetTask : forwardTasks) {
      basicOutputCollector.emitDirect(targetTask, FORWARD_TUPLE_STREAM_ID, values);
    }
  }

  public void store(Tuple tuple) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    String key = tuple.getStringByField("key");
    String value = tuple.getStringByField("value");

    Values values = new Values(rel, ts, key, value);

    if (rel.equals("R")) {
      activeSubIndexR.add(values);
      if (activeSubIndexR.getSize() >= subIndexSize) {
        indexChainR.add(activeSubIndexR);
        activeSubIndexR = new SubIndex();
      }
    } else if (rel.equals("S")) {
      activeSubIndexS.add(values);
      if (activeSubIndexS.getSize() >= subIndexSize) {
        indexChainS.add(activeSubIndexS);
        activeSubIndexS = new SubIndex();
      }
    }
  }

  public void join(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    if (rel.equals("R")) {
      join(tuple, indexChainS, activeSubIndexS, basicOutputCollector);
    } else if (rel.equals("S")) {
      join(tuple, indexChainR, activeSubIndexR, basicOutputCollector);
    }
  }

  public void join(Tuple tuple, Queue<SubIndex> indexChain, SubIndex activeSubIndex,
    BasicOutputCollector basicOutputCollector) {
    long ts = tuple.getLongByField("timestamp");
    Iterator<SubIndex> iterator = indexChain.iterator();

    while (iterator.hasNext()) {
      SubIndex subIndex = iterator.next();
      if (!isWindowJoin || (isInWindow(ts, subIndex.getMaxTimeStamp())) || isInWindow(ts, subIndex.getMinTimeStamp())) {
        join(tuple, subIndex, basicOutputCollector);
      }
    }
    join(tuple, activeSubIndex, basicOutputCollector);
  }

  public void join(Tuple tuple, SubIndex subIndex, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    String key = tuple.getStringByField("key");
    long ts = tuple.getLongByField("timestamp");
    for (Values storedTuple : subIndex.getTuplesByKey(key)) {
      if (!isWindowJoin || isInWindow(ts, getLong(storedTuple.get(1)))) {
        long tsStoredTuple = tuple.getLongByField("timestamp");
//        long temp = System.currentTimeMillis() - Math.max(ts, tsStoredTuple);
//        if (temp > 0) {
//          latency += temp;
//        }
        latency += (System.currentTimeMillis() - Math.max(ts, tsStoredTuple));
        numResults++;
        String value = tuple.getStringByField("value");
        String storedValue = getString(storedTuple.get(3));
        String joinedResult = rel.equals("R") ? (value + ";" + storedValue) : (storedValue + ";" + value);
        long sec = stopWatch.elapsed(TimeUnit.SECONDS);
        if (checkDuplicate && sec >50 && sec <= 150) {
          basicOutputCollector.emit(JOIN_RESULTS_STREAM_ID, new Values(joinedResult, rel));
        }
      }
    }
  }


  public boolean isInWindow(long tsNewTuple, long tsStoredTuple) {
    return Math.abs(tsNewTuple - tsStoredTuple) <= windowLength;
  }

  public boolean isTimeToOutputProfile() {
    long currentTime = stopWatch.elapsed(TimeUnit.SECONDS);
    if (currentTime >= triggerReportInSeconds) {
      triggerReportInSeconds = currentTime + profileReportInSeconds;
      return true;
    } else {
      return false;
    }
  }


}
