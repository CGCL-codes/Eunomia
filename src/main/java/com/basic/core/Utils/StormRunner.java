package com.basic.core.Utils;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
public class StormRunner {
  public static final int MILLIS_IN_SEC = 1000;

  public static void runLocal(String topologyName, StormTopology topology, Config conf, int runTime)
    throws InterruptedException {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, topology);
    Thread.sleep((long) runTime * MILLIS_IN_SEC);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

  public static void runCluster(String topologyName, StormTopology topology, Config conf)
    throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
    StormSubmitter.submitTopology(topologyName, conf, topology);
  }
}
