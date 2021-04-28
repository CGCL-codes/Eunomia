package com.basic.core.Component;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;

public class CollectBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(CollectBolt.class);
  long numAllJoin;
  long numDuplicatedJoin;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    numAllJoin = 0l;
    numDuplicatedJoin = 0l;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Long all = tuple.getLongByField("all");
    Long duplicate = tuple.getLongByField("duplicate");
    numAllJoin += all;
    numDuplicatedJoin += duplicate;
    if (numAllJoin % 1000000 == 0) {
      LOG.info(numAllJoin + " " + numDuplicatedJoin);
    }

  }
}
