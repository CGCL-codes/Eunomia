package com.basic.core.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static com.basic.core.Utils.Config.*;

public class ControllerBolt extends BaseBasicBolt {

  private List<Integer> joinerTasks;

  private List<List<Integer>> router;
  private List<List<Integer>> forest;
  private HashMap<Integer, Integer> joinerIndex;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);

    joinerTasks = context.getComponentTasks(JOINER_BOLT_ID);
    joinerIndex = new HashMap<>();
    for (int i = 0; i < joinerTasks.size(); i++) {
      joinerIndex.put(joinerTasks.get(i), i);
    }
    router = new ArrayList<>();
    forest = new ArrayList<>();

  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {

    int root =  tuple.getIntegerByField("root");
    int degree = tuple.getIntegerByField("degree");
    adjustJoinTree(root, degree);
    List<Integer> roots = new ArrayList<>();
    for (int i = 0; i < forest.size(); i++) {
      roots.add(joinerTasks.get(forest.get(i).get(0)));
    }
    collector.emit(ROUTE_TABLE_DISPATCHER_STREAM_ID, new Values("adjust", root, degree, roots, router));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(ROUTE_TABLE_DISPATCHER_STREAM_ID, new Fields("relation", "root", "degree", "roots", "router"));
  }

  public void adjustJoinTree(int root, int degree) {
    forest.clear();
    router.clear();
    int num = joinerTasks.size();
    int numJoinersTree = (int) Math.ceil((double) num / root);

    for (int i = 0; i < root; i++) {
      forest.add(new ArrayList<>());
      for (int j = 0; j < numJoinersTree; j++) {
        forest.get(i).add(-1);
      }
    }

    for (int i = 0; i < num; i++) {
      router.add(new ArrayList<>());
      for (int j = 0; j < degree; j++) {
        router.get(i).add(-1);
      }
      int n = i / root;
      int m = i % root;
      forest.get(m).set(n, i);
    }

    for (int i = 0; i < root; i++) {
      boolean flag = false;
      for (int j = 0; j < numJoinersTree; j++) {
        for (int d = 1; d <= degree; d++) {
          int child = j * degree + d;
          if (child >= numJoinersTree || forest.get(i).get(child) == -1) {
            flag = true;
            break;
          }
          router.get(forest.get(i).get(j)).set(d - 1, joinerTasks.get(forest.get(i).get(child)));
        }
        if (flag) {
          break;
        }
      }
    }
  }
}
