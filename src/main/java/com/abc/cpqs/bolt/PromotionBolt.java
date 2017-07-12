package com.abc.cpqs.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.abc.cpqs.dao.UserDAO;
import com.abc.cpqs.domain.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lijiax on 6/13/17.
 */
public class PromotionBolt extends BaseBasicBolt {
    private static Logger logger = LoggerFactory.getLogger(PromotionBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void execute(Tuple tuple,BasicOutputCollector collector) {
        String out = tuple.getString(0).trim();
        int id = Integer.parseInt(out) * 10;
        logger.info("Bolt got a tuple " + out);
        User user = UserDAO.getUser(id);

        String userStr;
        if (null != user) {
            userStr = "query id is "+id+" and "+user.toString();
            logger.info(userStr);
        } else {
            //userStr = "query id is "+id+" no result \n";
            logger.info("query id is "+id+" and result is empty.");
        }
    }

}