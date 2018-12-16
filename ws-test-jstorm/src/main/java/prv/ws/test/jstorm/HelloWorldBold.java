package prv.ws.test.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Hello world!
 */
public  class HelloWorldBold extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(HelloWorldBold.class);
    private OutputCollector collector;

    public HelloWorldBold(){
        LOGGER.info("HelloWorldBold.HelloWorldBold()");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        LOGGER.info("HelloWorldBold.prepare()");
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        LOGGER.info("HelloWorldBold.execute(),input="+input.getValues());
        String xx = input.getStringByField("value111");
        LOGGER.info("value111={}", xx);
        this.collector.ack(input);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOGGER.info("HelloWorldBold.declareOutputFields()");
    }
}
