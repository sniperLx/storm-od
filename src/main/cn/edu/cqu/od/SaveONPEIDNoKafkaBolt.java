package cn.edu.cqu.od;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by lx on 2016/10/26.
 */

/**
 * Save One Node Path into DB Bolt without kafka
 * If one eid's path chain length is smaller than one, it will be send to here. And save into DB.
 */
public class SaveONPEIDNoKafkaBolt implements IRichBolt {
    private static final long serialVersionUID = -4125083170338130703L;

    private Connection conn = null;
    private PreparedStatement pre = null;

    private OutputCollector _collector = null;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //just for test
        // declarer.declare(new Fields("eid"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        indexed_open();

    }

    private void indexed_open() {
        final String host_port = "10.0.0.2:3307";
        final String database = "od_week";
        final String user = "lx";
        final String password = "xxxx";
        final String url = "jdbc:mysql://" + host_port + "/" + database;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple input) {
        /**
         *If exist, update it.
         */
        String sql = "INSERT INTO one_node_path_eid(EID) VALUES (?) ON DUPLICATE KEY UPDATE EID=?";
        try {
            pre = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String eid = input.getStringByField("eid");
        try {
            pre.setString(1, eid);
            pre.setString(2, eid);
            pre.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        _collector.ack(input);

    }

    public void cleanup() {
        try {
            if (pre != null) {
                pre.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
