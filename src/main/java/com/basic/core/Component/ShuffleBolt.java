package com.basic.core.Component;

import com.basic.core.Utils.GeoHash;
import com.basic.core.Utils.StopWatch;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;

import static com.basic.core.Utils.Config.R_STREAM_TOPIC;
import static com.basic.core.Utils.Config.SHUFFLE_BOLT_ID;
import static com.basic.core.Utils.Config.S_STREAM_TOPIC;
import static com.basic.core.Utils.TimeUtils.*;

import static com.basic.core.Utils.Config.SCHEMA;
import static com.basic.core.Utils.CastUtils.getLong;
import static org.slf4j.LoggerFactory.getLogger;

public class ShuffleBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(ShuffleBolt.class);
  private long numRTuples;
  private long numSTuples;

  private String streamR;
  private String streamS;
  private Long last = 0l;
  private StopWatch stopWatch;

  private int count = 0;
  private int tuplesPerTime;
  private int numShufflers;

  public ShuffleBolt(int rate) {
    super();
    tuplesPerTime = rate / 10;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    numRTuples = 0;
    numSTuples = 0;
    streamR = R_STREAM_TOPIC;
    streamS = S_STREAM_TOPIC;

    stopWatch = StopWatch.createStarted();
    numShufflers = context.getComponentTasks(SHUFFLE_BOLT_ID).size();
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    long cur = stopWatch.elapsed(TimeUnit.MILLISECONDS);
    if (cur - last > 100) {
      count = 0;
      last = cur;
    } else if (count >= tuplesPerTime) {
      count = 0;
      Utils.sleep(100 - (cur - last));
      last = stopWatch.elapsed(TimeUnit.MILLISECONDS);
    }


      String topic = tuple.getStringByField("topic");
      String value = tuple.getStringByField("value");
      Long ts =  System.currentTimeMillis();
//    String value = tuple.getStringByField("value").split(" ")[0];
//    Long ts = getLong(tuple.getStringByField("value").split(" ")[1]);
      String[] values = value.split(",");

      if (topic.equals(streamR)) {
        String rel = "R";
        String key = GeoHash.encode(Double.parseDouble(values[4]), Double.parseDouble(values[3]), 7).toHashString();
//        String key = values[0];
        numRTuples++;
        basicOutputCollector.emit(new Values(rel, ts, key, value));
      } else if (topic.equals(streamS)) {
        String rel = "S";
        String key = GeoHash.encode(Double.parseDouble(values[4]), Double.parseDouble(values[3]), 7).toHashString();
//        String key = values[0];
        numSTuples++;
        basicOutputCollector.emit(new Values(rel, ts, key, value));
      }
      count++;


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("relation", "timestamp", "key", "value"));
  }
}
