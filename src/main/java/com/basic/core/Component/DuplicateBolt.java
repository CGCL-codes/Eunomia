package com.basic.core.Component;

import static org.slf4j.LoggerFactory.getLogger;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import static com.basic.core.Utils.Config.DUPLICATE_STREAM_ID;

public class DuplicateBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(CollectBolt.class);
  long numAllJoin;
  long numDuplicatedJoin;
  BloomFilter<String> bloomFilter;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    numAllJoin = 0l;
    numDuplicatedJoin = 0l;
    bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 30000000, 0.001);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declareStream(DUPLICATE_STREAM_ID, new Fields("all", "duplicate"));
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String value = tuple.getStringByField("value");
    numAllJoin++;
    if (bloomFilter.mightContain(value)) {
      numDuplicatedJoin++;
    } else {
      bloomFilter.put(value);
    }
    if (numAllJoin == 1000) {
      collector.emit(DUPLICATE_STREAM_ID, new Values(numAllJoin, numDuplicatedJoin));
      numAllJoin = 0;
      numDuplicatedJoin = 0;
    }
  }
}
