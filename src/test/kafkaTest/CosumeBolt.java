package kafkaTest;

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
 * Created by lab on 2016/5/29.
 */
public class CosumeBolt implements IRichBolt {

    private Connection conn = null;
    private PreparedStatement pre = null;

    OutputCollector _collector = null;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String host_port = "10.0.0.1:3306";           //remote node1
        String database = "od_week";
        String user = "lx";
        String password = "xxxx";
        String url = "jdbc:mysql://" + host_port + "/" + database;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        _collector = collector;
    }

    public void execute(Tuple input) {
        String eid = input.getStringByField("str");
        String sql = "INSERT INTO kafka_eid_test VALUES (?) ON DUPLICATE KEY UPDATE EID=?";
        try {
            pre = conn.prepareStatement(sql);
            pre.setString(1, eid);
            pre.setString(2, eid);
            pre.executeUpdate();
            _collector.ack(input);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void cleanup() {
        try {
            if (pre != null)
                pre.close();
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
