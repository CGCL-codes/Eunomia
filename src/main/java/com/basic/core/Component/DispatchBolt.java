package com.basic.core.Component;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import static com.basic.core.Utils.Config.*;

public class DispatchBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(DispatchBolt.class);
  private int taskId;
  private List<Integer> joinerTasks;
  private List<Integer> dispatcherTasks;
  private List<Integer> rootJoiners;
  private long tupleId;
  private int root;
  private String status = "normal";
  private List<Values> bufferedTuples;
  private int numAckJoiners = 0;
//  private Set<String> emitTuples;

  public DispatchBolt(int r) {
    root = r;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    taskId = context.getThisTaskId();
    dispatcherTasks = context.getComponentTasks(DISPATCHER_BOLT_ID);
    joinerTasks = context.getComponentTasks(JOINER_BOLT_ID);
    tupleId = 0l;
    rootJoiners = new ArrayList<>();
    if (joinerTasks != null && joinerTasks.size() != 0) {
      for (int i = 0; i < root && i < joinerTasks.size(); i++) {
        rootJoiners.add(joinerTasks.get(i));
      }
    }
    bufferedTuples = new ArrayList<>();
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");

    if (rel.equals("adjust")) {
      int r = tuple.getIntegerByField("root");
      int d = tuple.getIntegerByField("degree");
      List<Integer> roots = (List<Integer>) tuple.getValueByField("roots");
      List<List<Integer>> router = (List<List<Integer>>) tuple.getValueByField("router");

      for (int otherDispatcher : dispatcherTasks) {
        if (otherDispatcher != taskId) {
          Values values = new Values("status change", r, d, roots);
          basicOutputCollector.emitDirect(otherDispatcher, STATUS_CHANGE_STREAM_ID, values);
        }
      }

      for (int i = 0; i < root; i++) {
        Values values = new Values(r, d, router);
        basicOutputCollector.emitDirect(rootJoiners.get(i), ROUTE_TABLE_JOINER_STREAM_ID, values);
      }

      status = "adjusting";
      rootJoiners = roots;
      root = r;

    } else if (rel.equals("status change")) {
      int r = tuple.getIntegerByField("root");
      List<Integer> roots = (List<Integer>) tuple.getValueByField("roots");
      status = "adjusting";
      rootJoiners = roots;
      root = r;

    } else if (rel.equals("ack")) {
      numAckJoiners++;
      if (numAckJoiners == joinerTasks.size() && status.equals("adjusting")) {
        long barrier = System.currentTimeMillis() / 10;
        for (Values values : bufferedTuples) {
          values.add(5, barrier);
          for (int i = 0; i < root; i++) {
            basicOutputCollector.emitDirect(rootJoiners.get(i), TUPLE_STREAM_ID, values);
          }
        }
        bufferedTuples.clear();
        status = "normal";
        numAckJoiners = 0;
      }
    } else if (rel.equals("R") || rel.equals("S")) {
      int target = joinerTasks.get((int)(tupleId % joinerTasks.size()));
      Long ts = tuple.getLongByField("timestamp");
      String key = tuple.getStringByField("key");
      String value = tuple.getStringByField("value");
      long barrier = System.currentTimeMillis() / 10;
      Values values = new Values(target, rel, ts, key, value, barrier);
      if (status.equals("adjusting")) {
        bufferedTuples.add(new Values(target, rel, ts, key, value));
      } else if (status.equals("normal")) {
        for (int i = 0; i < root; i++) {
          basicOutputCollector.emitDirect(rootJoiners.get(i), TUPLE_STREAM_ID, values);
        }
      }


      tupleId++;
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(TUPLE_STREAM_ID, new Fields("target", "relation", "timestamp", "key", "value", "barrier"));
    declarer.declareStream(STATUS_CHANGE_STREAM_ID, new Fields("relation", "root", "degree", "roots"));
    declarer.declareStream(ROUTE_TABLE_JOINER_STREAM_ID, new Fields("root", "degree", "router"));
  }

  @Override
  public void cleanup() {

  }
}
