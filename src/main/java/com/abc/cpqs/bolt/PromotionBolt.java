package com.abc.cpqs.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.abc.cpqs.dao.UserDAO;
import com.abc.cpqs.domain.User;
import com.abc.cpqs.util.PropertyLoader;
import com.abc.cpqs.util.SocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by lijiax on 6/13/17.
 */
public class PromotionBolt extends BaseRichBolt {
    private static Logger logger = LoggerFactory.getLogger(PromotionBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void execute(Tuple tuple) {
        String out = tuple.getString(0);
        logger.info("bolt got tuple "+out);
        //query db
        User user = UserDAO.getUser(Integer.parseInt(out));
        logger.info("query db result is "+user);
        Properties conf = PropertyLoader.getProperties("/home/lijiax/conf/socket.properties");
        String ip = conf.getProperty("promotion.ip");
        int port = Integer.parseInt(conf.getProperty("promotion.port"));
        SocketClient client = new SocketClient(ip, port);
        try {
            client.send(user.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
