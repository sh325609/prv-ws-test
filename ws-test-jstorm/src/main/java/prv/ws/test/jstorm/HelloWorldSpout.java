package prv.ws.test.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Hello world!
 *
 */
public class HelloWorldSpout extends BaseRichSpout {
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloWorldSpout.class);
    private SpoutOutputCollector collector;

    public HelloWorldSpout(){
        LOGGER.info("HelloWorldSpout.HelloWorldSpout()");
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOGGER.info("HelloWorldSpout.open()");
        this.collector = collector;
    }

    int i=0;

    @Override
    public void nextTuple() {
        LOGGER.info("HelloWorldSpout.nextTuple()");
            this.collector.emit(new Values("Value-"+i));
            i++;
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        LOGGER.info("HelloWorldSpout.declareOutputFields()");
        declarer.declare(new Fields("value111"));

    }

    /**
     * 启用 ack 机制，详情参考：https://github.com/alibaba/jstorm/wiki/Ack-%E6%9C%BA%E5%88%B6
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        LOGGER.info("HelloWorldSpout.ack(),msgId.class=" + msgId + ",msgId=" + msgId);
        super.ack(msgId);
    }

    /**
     * 消息处理失败后需要自己处理
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        LOGGER.info("HelloWorldSpout.fail(),msgId.class=" + msgId + ",msgId=" + msgId);
        super.fail(msgId);
    }


    public void close(){
        LOGGER.info("HelloWorldSpout.close()");
    }

    public void activate(){
        LOGGER.info("HelloWorldSpout.activate()");
    }

    public void deactivate(){
        LOGGER.info("HelloWorldSpout.deactivate()");
    }

}
