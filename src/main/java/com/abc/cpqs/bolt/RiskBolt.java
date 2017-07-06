package com.abc.cpqs.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.abc.cpqs.util.PropertyLoader;
import com.abc.cpqs.util.SocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;


/**
 * Created by lijiax on 6/9/17.
 * bolt to operate tuples from spout
 */
public class RiskBolt extends BaseRichBolt {
    private static Logger logger = LoggerFactory.getLogger(RiskBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void execute(Tuple tuple) {
        String out = tuple.getString(0);
        String str = "Get a string in riskbolt: "+out;
        logger.info("Get a string in riskbolt: "+out);
        logger.info("////////////////////////////////////////");
        Properties conf = PropertyLoader.getProperties("/home/lijiax/conf/socket.properties");
        String ip = conf.getProperty("risk.ip");
        int port = Integer.parseInt(conf.getProperty("risk.port"));
        SocketClient client = new SocketClient(ip, port);
        try {
            client.send(str);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
