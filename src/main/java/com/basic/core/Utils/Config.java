package com.basic.core.Utils;

import com.basic.core.Topology;
import com.basic.core.TopologyArgs;
import com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

public class Config {
  public static final List<String> SCHEMA = ImmutableList.of("target", "relation", "timestamp", "key", "value");

  public static final String R_STREAM_TOPIC = "Gps1";
  public static final String S_STREAM_TOPIC = "Orders1";
//cat worker.log | grep 'metric' | awk '{print $10 "\t" $11}'
  public static final String KAFKA_SPOUT_ID_R ="kafka-spout-r";
  public static final String KAFKA_SPOUT_ID_S ="kafka-spout-s";
  public static final String SHUFFLE_BOLT_ID = "shuffle";
  public static final String DISPATCHER_BOLT_ID = "dispatcher";
  public static final String JOINER_BOLT_ID = "joiner";
  public static final String DUPLICATE_BOLT_ID = "duplicate";
  public static final String COLLECTOR_BOLT_ID = "collector";
  public static final String METRIC_BOLT_ID = "metric";
  public static final String CONTROLLER_BOLT_ID = "controller";

  public static final String TUPLE_STREAM_ID = "tuples";
  public static final String SHUFFLE_R_STREAM_ID = "shuffle-r";
  public static final String SHUFFLE_S_STREAM_ID = "shuffle-s";
  public static final String BROADCAST_R_STREAM_ID = "broadcast-r";
  public static final String BROADCAST_S_STREAM_ID = "broadcast-s";
  public static final String JOIN_RESULTS_STREAM_ID = "join-results";
  public static final String METRIC_STREAM_ID = "metric";
  public static final String DUPLICATE_STREAM_ID = "duplicate";
  public static final String ROUTE_TABLE_DISPATCHER_STREAM_ID = "route_table_dispatcher";
  public static final String ROUTE_TABLE_JOINER_STREAM_ID = "route_table_joiner";
  public static final String STATUS_CHANGE_STREAM_ID = "status_changer";
  public static final String FORWARD_TUPLE_STREAM_ID = "forward_tuple";
  public static final String ACK_TO_DISPATCHER_STREAM_ID = "ack_to_dispatcher";
  public static final String STEP_TWO_STREAM_ID = "step_two";


}
