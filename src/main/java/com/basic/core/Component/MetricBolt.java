package com.basic.core.Component;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static com.basic.core.Utils.CastUtils.getLong;
import static org.slf4j.LoggerFactory.getLogger;

import com.basic.core.Utils.StopWatch;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

public class MetricBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(MetricBolt.class);

  private static final List<String> SCHEMA = ImmutableList.of("count", "sum", "min", "max");

  private long currentTime;
  private StopWatch stopwatch;
  private Map<Integer, Queue<Values>> statistics;
  private long numJoiners;
  private int sec;

  public MetricBolt() {
    super();
    statistics = new HashMap<>();
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    numJoiners = context.getComponentTasks("joiner").size();
    stopwatch = StopWatch.createStarted();
    currentTime = stopwatch.elapsed(MICROSECONDS);
    sec = 1;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Values values = new Values(tuple.getLongByField("currentMoment"), tuple.getLongByField("tuples"),
                               tuple.getLongByField("results"), tuple.getLongByField("processingDuration"),
                               tuple.getDoubleByField("latency"));

    int taskId = tuple.getSourceTask();
    if (!statistics.containsKey(taskId)) {
      statistics.put(taskId, new LinkedList<>());
    }
    statistics.get(taskId).offer(values);
    if (statistics.size() == numJoiners) {
      LinkedList<Integer> list = new LinkedList<>();
      long tuples = 0;
      long results = 0;
      long processingDuration = 0;
      double latency = 0;
      double ratio = numJoiners;
      int num = 0;
      for (Entry<Integer, Queue<Values>> entry : statistics.entrySet()) {
        Values temp = entry.getValue().poll();
        tuples += getLong(temp.get(1));
        results += getLong(temp.get(2));
        processingDuration += getLong(temp.get(3));
        latency += ((Double)temp.get(4)).doubleValue();
        num++;

        if (entry.getValue().isEmpty()) {
          list.add(entry.getKey());
        }
      }
      for (Integer key : list) {
        statistics.remove(key);
      }
      StringBuffer sb = new StringBuffer();
      sb.append("@[" + sec + " sec], " + num + ": ");
//      if (sec == 30) {
//        collector.emit(new Values(1));
//      }

      double throughput = tuples;
      if (processingDuration == 0) {
        throughput = 0;
      } else {
        throughput = throughput / processingDuration * num * 1000;
        throughput /= ratio;
      }
      sb.append(String.format("%.2f ", throughput));
      if (results == 0) {
        latency = 0;
      } else {
        latency /= results;
      }
      sb.append(String.format("%.6f", latency));
      LOG.info(sb.toString());
    }
    /*while (statistics.size() == boltsNum) {
      for (Entry<Integer, Queue<Values>> entry : statistics.entrySet()) {
        Values temp = entry.getValue().peek();
        Long tempTs = getLong(temp.get(0));
        boolean empty = false;
        while (tempTs.compareTo(ts) < 0) {
          entry.getValue().poll();
          if (entry.getValue().isEmpty()) {
            statistics.remove(entry.getKey());
            empty = true;
            break;
          }
          temp = entry.getValue().peek();
          tempTs = getLong(temp.get(0));
        }
        if (empty) {
          break;
        }
        ts = tempTs;
      }
      if (statistics.size() == boltsNum) {
        boolean equal = true;
        for (Entry<Integer, Queue<Values>> entry : statistics.entrySet()) {
          Values temp = entry.getValue().peek();
          Long tempTs = getLong(temp.get(0));
          if (!tempTs.equals(ts)) {
            equal = false;
            break;
          }
        }
        long cur = stopwatch.elapsed(MICROSECONDS);
        boolean hasOutput = false;
        if (cur - currentTime >= 1000 && equal) {
          long tuples = 0;
          long processingDuration = 0;
          double latency = 0;
          LinkedList<Integer> list = new LinkedList<>();
          int num = 0;

          for (Entry<Integer, Queue<Values>> entry : statistics.entrySet()) {
            if (!entry.getValue().isEmpty()) {
              Values temp = entry.getValue().poll();
              if (entry.getValue().isEmpty()) {
                list.add(entry.getKey());
              }
              tuples += getLong(temp.get(1));
              processingDuration += getLong(temp.get(3));
              latency += ((Double)temp.get(4)).doubleValue();
              num++;
            }
          }
          for (Integer key : list) {
            statistics.remove(key);
          }
          StringBuffer sb = new StringBuffer();
          sb.append("@ [" + ts + " sec], ");
          double throughput = tuples;
          if (processingDuration == 0) {
            throughput = 0;
          } else {
            throughput = throughput / processingDuration * num * 1000;
          }
          sb.append(String.format("%.2f, ", throughput));
          if (tuples == 0) {
            latency = 0;
          } else {
            latency /= tuples;
          }
          sb.append(String.format("%.6f", latency));
          LOG.info(sb.toString());
          hasOutput = true;
        }
        if (hasOutput) {
          currentTime = stopwatch.elapsed(MILLISECONDS);
        }
      }

    }*/
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("rate_change", new Fields("rate"));
    declarer.declareStream("adjust", new Fields("root", "degree"));
  }
}
