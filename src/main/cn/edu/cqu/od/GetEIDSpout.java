package cn.edu.cqu.od;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by lab on 2016/4/26.
 *
 * @author lx
 */

//read single car's path chain into memory.
//which has been sorted by 'time' field in db.
//This class was used to produce EID data, now we kafka to replace it.
public class GetEIDSpout extends BaseRichSpout {
    public static final Logger LOG = LoggerFactory.getLogger(GetEIDSpout.class);

    //save messages not acked.
    private static HashMap<String, String> MessagesNotAcked = new HashMap<String, String>();

    private static final int queueSize = 1024000;
    private static LinkedBlockingDeque<String> eid_queue = new LinkedBlockingDeque<String>(queueSize);
    private SpoutOutputCollector _collector;

    private Connection conn = null;
    private PreparedStatement pre = null;
    private ResultSet resultSet = null;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("eid", "single-car-path-chain"));
        declarer.declare(new Fields("eid"));
    }

    public void open(Map paramMap, TopologyContext paramTopologyContext, SpoutOutputCollector collector) {
        //when starting a topology , this method will be involved by storm.
        //open connection to database.
        indexed_open();
        this._collector = collector;
        System.err.println("open");
    }

    void indexed_open() {
        final String host_port = "10.0.0.1:3306";
        final String database = "od_week";
        final String user = "lx";
        final String password = "password_mysql";
        final String url = "jdbc:mysql://" + host_port + "/" + database;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String sql = "select * from eid";
        try {
            pre = conn.prepareStatement(sql);
            //avoid outofmemory error
            pre.setFetchSize(Integer.MIN_VALUE);

            resultSet = pre.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //create a new thread, read eid from file and put it into LinkedBlockingQueue
        Thread producer = new Thread(new Runnable() {
            public void run() {
                try {
                    while (resultSet.next()) {
                        String eid = resultSet.getString("EID");
                        //control output speed
                        Utils.sleep(4);
                        eid_queue.put(eid);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        producer.start();
        LOG.info("hello from open in GetEIDSpout");
    }

    public void nextTuple() {
        //storm will execute this method again and again.
        //get data from database, do something and emit to next bolt.
        String eid = eid_queue.poll();
        if (eid != null) {
            String msgId = eid + ":\t" + new Random().nextDouble();
            List<Object> tuple = new Values(eid);
            _collector.emit(tuple, msgId);
            //remember not acked message
            MessagesNotAcked.put(msgId, eid);
        }
    }

    @Override
    public void ack(Object msgId) {
        MessagesNotAcked.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        //resend failed message
        String failed_msg = MessagesNotAcked.get(msgId);
        try {
            if (!eid_queue.contains(failed_msg))
                eid_queue.put(failed_msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("in GetEIDSpout fail " + msgId);
    }

    @Override
    public void close() {
        //when the topology is killed, this method will be involved by storm.
        //close connection to database
        try {
            if (resultSet != null)
                resultSet.close();
            if (pre != null)
                pre.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
