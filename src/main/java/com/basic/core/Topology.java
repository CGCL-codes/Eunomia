package com.basic.core;

import static com.basic.core.Utils.Config.*;
import static com.basic.core.Utils.StormRunner.runCluster;
import static com.basic.core.Utils.StormRunner.runLocal;
import static org.slf4j.LoggerFactory.getLogger;

import com.basic.core.Component.CollectBolt;
import com.basic.core.Component.ControllerBolt;
import com.basic.core.Component.DispatchBolt;
import com.basic.core.Component.DuplicateBolt;
import com.basic.core.Component.JoinBolt;
import com.basic.core.Component.MetricBolt;
import com.basic.core.Component.RandomDataSpout;
import com.basic.core.Component.ShuffleBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

public class Topology {

  public static final String KAFKA_BROKER = "node24:9092,node26:9092,node30:9092";
  private static final Logger LOG = getLogger(Topology.class);
  private final TopologyArgs topologyArgs = new TopologyArgs(Topology.class.getName());

  public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String servers, String topic, String groupId,
    int rate) {
    ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
      (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
      new Fields("topic", "partition", "offset", "key", "value"));
    return KafkaSpoutConfig.builder(servers, new String[]{topic})
      .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      .setRetry(getRetryService())
      .setRecordTranslator(trans)
      .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
      .setProcessingGuarantee(ProcessingGuarantee.AT_MOST_ONCE)
      .build();
  }

  public static KafkaSpoutRetryService getRetryService() {
    return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
      KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
      KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
  }

  public static void main(String[] args) throws Exception {
    (new Topology()).run(args);

  }

  public void run(String[] args) throws Exception {
    if (!topologyArgs.processArgs(args)) {
      return;
    }
    if (topologyArgs.help) {
      return;
    }

    StormTopology topology = createTopology(topologyArgs.remoteMode);
    if (topology == null) {
      LOG.error("Topology create failed!");
      return;
    }

    Config config = configureTopology();
    if (config == null) {
      LOG.error("Configure failed!");
      return;
    }

    if (topologyArgs.remoteMode) {
      LOG.info("execution mode: remote");
      runCluster(topologyArgs.topologyName, topology, config);
    } else {
      LOG.info("execution mode: local");
      runLocal(topologyArgs.topologyName, topology, config, topologyArgs.localRuntime);
    }
//3 145 25 26 34 35 67 68 75 96 97

  }

  public StormTopology createTopology(boolean remoteMode) {
    TopologyBuilder builder = new TopologyBuilder();

    if (remoteMode) {
      builder.setSpout(KAFKA_SPOUT_ID_R,
        new KafkaSpout<>(getKafkaSpoutConfig(KAFKA_BROKER, R_STREAM_TOPIC, topologyArgs.groupId, topologyArgs.rate)),
        topologyArgs.numKafkaSpouts);
      builder.setSpout(KAFKA_SPOUT_ID_S,
        new KafkaSpout<>(getKafkaSpoutConfig(KAFKA_BROKER, S_STREAM_TOPIC, topologyArgs.groupId, topologyArgs.rate)),
        topologyArgs.numKafkaSpouts);
    } else {
      builder.setSpout(KAFKA_SPOUT_ID_R, new RandomDataSpout("R"), topologyArgs.numKafkaSpouts);
      builder.setSpout(KAFKA_SPOUT_ID_S, new RandomDataSpout("S"), topologyArgs.numKafkaSpouts);
    }

    builder.setBolt(SHUFFLE_BOLT_ID, new ShuffleBolt(topologyArgs.rate / topologyArgs.numShufflers), topologyArgs.numShufflers)
      .shuffleGrouping(KAFKA_SPOUT_ID_R)
      .shuffleGrouping(KAFKA_SPOUT_ID_S)
      .allGrouping(METRIC_BOLT_ID, "rate_change");

    builder.setBolt(DISPATCHER_BOLT_ID, new DispatchBolt(topologyArgs.root), topologyArgs.numDispatcher)
      .shuffleGrouping(SHUFFLE_BOLT_ID)
      .shuffleGrouping(CONTROLLER_BOLT_ID, ROUTE_TABLE_DISPATCHER_STREAM_ID)
      .directGrouping(DISPATCHER_BOLT_ID, STATUS_CHANGE_STREAM_ID)
      .allGrouping(JOINER_BOLT_ID, ACK_TO_DISPATCHER_STREAM_ID);

    JoinBolt joiner = new JoinBolt(topologyArgs.checkDuplicate, topologyArgs.root, topologyArgs.degree);
    builder.setBolt(JOINER_BOLT_ID, joiner, topologyArgs.numJoiners)
      .directGrouping(DISPATCHER_BOLT_ID, TUPLE_STREAM_ID)
      .directGrouping(JOINER_BOLT_ID, FORWARD_TUPLE_STREAM_ID)
      .directGrouping(DISPATCHER_BOLT_ID, ROUTE_TABLE_JOINER_STREAM_ID)
      .directGrouping(JOINER_BOLT_ID, ROUTE_TABLE_JOINER_STREAM_ID);


    builder.setBolt(METRIC_BOLT_ID, new MetricBolt(), 1)
      .shuffleGrouping(JOINER_BOLT_ID, METRIC_STREAM_ID);

    builder.setBolt(CONTROLLER_BOLT_ID, new ControllerBolt(), 1)
      .shuffleGrouping(METRIC_BOLT_ID, "adjust");

    if (topologyArgs.checkDuplicate) {
      builder.setBolt(DUPLICATE_BOLT_ID, new DuplicateBolt(), topologyArgs.numDuplicate)
        .fieldsGrouping(JOINER_BOLT_ID, JOIN_RESULTS_STREAM_ID, new Fields("value"));
      builder.setBolt(COLLECTOR_BOLT_ID, new CollectBolt(), 1)
        .shuffleGrouping(DUPLICATE_BOLT_ID, DUPLICATE_STREAM_ID);
    }
    return builder.createTopology();

  }

  public Config configureTopology() {
    Config conf = new Config();

    conf.setNumWorkers(topologyArgs.numWorkers);
    conf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);
    conf.put("SUB_INDEX_SIZE", topologyArgs.subindexSize);
    conf.put("WINDOW_ENABLE", topologyArgs.window);
    conf.put("WINDOW_LENGTH", topologyArgs.windowLength);
    return conf;

  }
}
