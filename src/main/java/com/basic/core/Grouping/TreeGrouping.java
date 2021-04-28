package com.basic.core.Grouping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import static com.basic.core.Utils.Config.*;

public class TreeGrouping implements CustomStreamGrouping {

  private List<Integer> joinerTasks;
  private int root;
  private int degree;
  private int numJoiners;
  private int [][] router;
  private int [][] forest;
  private int numJoinersTree;
  private HashMap<Integer, Integer> joinerIndex;

  public TreeGrouping(int d, int n, int r) {
    degree = d;
    numJoiners = n;
    root = r;
    router = new int[n][d];
    numJoinersTree = (int) Math.ceil((double) n / r);
    forest = new int[r][numJoinersTree];
    joinerIndex = new HashMap<>();

    for (int i = 0; i < r; i++) {
      for (int j = 0; j < numJoinersTree; j++) {
        forest[i][j] = -1;
      }
    }
  }

  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
    joinerTasks = context.getComponentTasks(JOINER_BOLT_ID);

    for (int i = 0; i < numJoiners; i++) {
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
        for (int d = 1; d <= degree; d++) {
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


  }

  @Override
  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    List<Integer> targets = new ArrayList<>();
    int index = joinerIndex.get(taskId);
    for (int i = 0; i < degree; i++) {
      if (router[index][i] != -1) {
        targets.add(router[index][i]);
      }
    }
    return targets;
  }
}
