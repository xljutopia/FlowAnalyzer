package com.abc.cpqs;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.abc.cpqs.bolt.PromotionBolt;
import storm.kafka.*;

/**
 * Created by lijiax on 6/13/17.
 */
public class FlowAnalyzerTopology {

    public static void main(String[] args){
        //Properties config = PropertyLoader.getProperties("/home/lijiax/conf/kafka.properties");
        String zks = "localhost:2181,localhost:2182,localhost:2183";
        String topic = args[0];
		System.out.println("Topic name is "+topic);
        String zkRoot = "/"+topic;
        String id = "udpconsumer";
        BrokerHosts hosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
        spoutConf.forceFromStart = false;
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);

        builder.setSpout("kafka-reader", kafkaSpout,2);
        builder.setBolt("promotionBolt", new PromotionBolt(),2).shuffleGrouping("kafka-reader");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        try {

            StormSubmitter.submitTopology("FlowAnalyzerTopology", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
