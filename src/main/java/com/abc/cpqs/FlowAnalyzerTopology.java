package com.abc.cpqs;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.abc.cpqs.bolt.PromotionBolt;
import com.abc.cpqs.bolt.RiskBolt;
import com.abc.cpqs.util.PropertyLoader;
import storm.kafka.*;

import java.util.Properties;

/**
 * Created by lijiax on 6/13/17.
 */
public class FlowAnalyzerTopology {

    public static void main(String[] args){
        Properties config = PropertyLoader.getProperties("/home/lijiax/conf/kafka.properties");
        String zks = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
        String topic = "test-topic";
        String zkRoot = "/storm";
        String id = "flow";
        BrokerHosts hosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf),2);
        //builder.setSpout("spout", new UDPmsgReader(), 4);
        builder.setBolt("riskBolt", new RiskBolt(), 4).fieldsGrouping("kafka-reader","risk",new Fields("flow"));
        builder.setBolt("promotionBolt", new PromotionBolt(), 4).fieldsGrouping("kafka-reader", "promotion", new Fields("flow"));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        try {
            StormSubmitter.submitTopology("FlowAnalyzerTopology", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
