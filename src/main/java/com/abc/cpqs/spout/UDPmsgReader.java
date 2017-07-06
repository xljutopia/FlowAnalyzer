package com.abc.cpqs.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.abc.cpqs.util.PropertyLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by lijiax on 6/9/17.
 * spout to receive udp packet
 */
public class UDPmsgReader extends BaseRichSpout {
    private static Logger logger = LoggerFactory.getLogger(UDPmsgReader.class);

    SpoutOutputCollector collector;

    private UDPClient client;

    private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
    }

    @Override
    public void activate(){
        client = new UDPClient();
        client.setStartInfo();
        client.start();
    }

    @Override
    public void deactivate(){
        client.setRunFlag(false);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("risk", new Fields("flow"));
        outputFieldsDeclarer.declareStream("promotion", new Fields("flow"));
    }

    @Override
    public void nextTuple() {
        String str = queue.poll();
        if (str == null ) {
            return;
        }else{
            collector.emit("risk", new Values(str));
            collector.emit("promotion", new Values(str));
        }
    }

    @Override
    public void close() {
        super.close();
    }

    public class UDPClient extends Thread{
        private int port;
        private String ip;
        private boolean isTrue;
        private DatagramSocket ds;
        private Properties conf;

        public void setStartInfo(){
            try {
                conf = PropertyLoader.getProperties("/home/lijiax/conf/socket.properties");
                ip = conf.getProperty("udp.ip");
                port = Integer.parseInt(conf.getProperty("udp.port"));
                isTrue = true;
                ds = new DatagramSocket(port);
            }catch (SocketException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run(){
            while (isTrue) {
                try {
                    byte[] buff = new byte[4096];
                    DatagramPacket dp = new DatagramPacket(buff, 0, buff.length);

                    ds.receive(dp);
                    String str = new String(dp.getData(), 0, dp.getLength());

                    queue.add(str);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            stopReceive();
        }

        public void stopReceive(){
            try{
                ds.close();
            } catch (Exception e) {
            }
        }

        public void setRunFlag(boolean runFlag){
            this.isTrue = runFlag;
        }
    }
}
