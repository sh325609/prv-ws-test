package prv.ws.test.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Hello world!
 *
 */
public class HelloWorldTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        demo1();
    }

    /**
     * 最简单的Demo
     */
    private static void demo1()throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testspout", new HelloWorldSpout(), 1);
        builder.setBolt("testbolt", new HelloWorldBold(), 2).shuffleGrouping("testspout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumAckers(1);
        conf.put(Config.NIMBUS_THRIFT_PORT, 7627);
        StormSubmitter.submitTopology("testtopology", conf, builder.createTopology());
    }

    /**
     * DRPC 同步Demo TODO 没写完
     */
    private static void demo2()throws AlreadyAliveException, InvalidTopologyException {

//        TopologyBuilder builder = new TopologyBuilder();
//        //开始的Spout
//        DRPCSpout drpcSpout = new DRPCSpout("exclamation");
//        builder.setSpout("drpc-input", drpcSpout,5);
//
//        //真正处理的Bolt
//        builder.setBolt("cpp", new XXXBolt(), 5)
//                .noneGrouping("drpc-input");
//
//        //结束的ReturnResults
//        builder.setBolt("return", new ReturnResults(),5).noneGrouping("cpp");
//
//        Config conf = new Config();
//        conf.setDebug(false);
//        conf.setMaxTaskParallelism(3);
//
//        try
//        {
//            StormSubmitter.submitTopology("exclamation", conf,builder.createTopology());
//        }
//        catch (Exception e)
//        {
//            e.printStackTrace();
//        }
    }

}
