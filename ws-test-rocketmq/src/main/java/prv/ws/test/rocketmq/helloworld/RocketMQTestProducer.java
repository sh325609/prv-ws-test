package prv.ws.test.rocketmq.helloworld;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashMap;
import java.util.Map;

/**-
 * https://www.jianshu.com/p/11e875074a8f
 */
public class RocketMQTestProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer();

        producer.setProducerGroup("RocketMQTestProducer");
        producer.setNamesrvAddr("47.110.233.124:9876");
        producer.start();

        for (int i = 0; i < 1; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                byte[] body = ("Hello RocketMQ " + 1).getBytes();
//                Message msg = new Message("TopicTest" , "TagA" , body);
                Message msg = new Message("TestTopic", "Tag1", "Key1", body);
//                msg.setDelayTimeLevel(0);//延时等级：1s，5s，10s，30s，1m，2m，3m，4m，5m，6m，7m，8m，9m，10m，20m，30m，1h，2h。  level=0，表示不延时。level=1，表示 1 级延时，对应延时 1s。level=2 表示 2 级延时，对应5s，以此类推
                SendResult sendResult = producer.send(msg,11111);
                Thread.sleep(1000);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}




















