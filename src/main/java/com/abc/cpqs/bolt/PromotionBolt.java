package com.abc.cpqs.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.abc.cpqs.dao.UserDAO;
import com.abc.cpqs.domain.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by lijiax on 6/13/17.
 */
public class PromotionBolt extends BaseRichBolt {
    private static Logger logger = LoggerFactory.getLogger(PromotionBolt.class);

    FileOutputStream fop = null;
    File file = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        file = new File("/home/lijiax/logs/storm_out.txt");
        try {
            fop = new FileOutputStream(file);
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void execute(Tuple tuple) {
        String out = tuple.getString(0).trim();
        logger.info("bolt got tuple " + out);
        User user = UserDAO.getUser(Integer.parseInt(out) * 10);
        //logger.info("query db result is "+user);
        /*Properties conf = PropertyLoader.getProperties("/home/lijiax/conf/socket.properties");
        String ip = conf.getProperty("promotion.ip");
        int port = Integer.parseInt(conf.getProperty("promotion.port"));
        SocketClient client = new SocketClient(ip, port);*/
        String userStr = null;
        if (null != user) {
            userStr = user.toString() + "\n";
            logger.info("query db result is " + userStr);
        } else {
            userStr = "no result \n";
            logger.info("query result is empty");
        }
        byte[] contentInBytes = userStr.getBytes();
        try {
            fop.write(contentInBytes);
            fop.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        try {
            if (fop != null) {
                fop.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}