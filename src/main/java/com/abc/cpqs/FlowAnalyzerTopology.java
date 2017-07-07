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
        String zks = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
        String topic = "udp";
        String zkRoot = "/storm";
        String id = "flow";
        BrokerHosts hosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf),2);
        builder.setBolt("promotionBolt", new PromotionBolt()).shuffleGrouping("kafka-reader");

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
