package cn.edu.cqu.od;

/**
 * Created by lab on 2016/5/19.
 *
 * @author lx
 */

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
 * Save One Node Path into DB Bolt
 * If one eid's path chain length is smaller than one, it will be send to here. And save into DB.
 */
public class SaveONPEIDBolt implements IRichBolt {
    private static final long serialVersionUID = 149500811764464501L;

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
