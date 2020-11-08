package cn.edu.cqu.od;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lx on 2016/10/26.
 */

//save single car's OD into database without kafka
public class SaveODNoKafkaBolt extends BaseRichBolt {
    private static final long serialVersionUID = 3481270695402858422L;
    private static Logger logger = Logger.getLogger(SavaODBolt.class);

    private Connection conn = null;
    private PreparedStatement pre = null;

    private OutputCollector _collector = null;

    public void declareOutputFields(OutputFieldsDeclarer paramOutputFieldsDeclarer) {
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String host_port = "10.0.0.2:3307";           //remote node1
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
        createTable();

    }

    void createTable() {
        //create table in db to save received contents.
        //if table does not exist, create it.
        String sql_create_table =
                "CREATE TABLE IF NOT EXISTS single_car_od (" +
                        // "id BIGINT AUTO_INCREMENT NOT NULL ," +
                        "eid varchar(10) NULL , " +
                        "od_O_ip varchar(20) NULL ," +
                        "od_D_ip varchar(20) NULL ," +
                        "od_O_time DATETIME NULL ," +
                        "od_D_time DATETIME NULL ," +
                        "car_type varchar(10) NULL ," +
                        "PRIMARY KEY(eid,od_O_ip,od_D_ip,od_O_time))";
        try {
            Statement statement = conn.createStatement();
            statement.executeUpdate(sql_create_table);
        } catch (SQLException e) {
            logger.error("create table single_car_od failed.");
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        //save result to  table named single_car_od
        saveToDB(tuple);
    }

    private void saveToDB(Tuple tuple) {
        String eid = (String) tuple.getValueByField("eid");
        //Be sure that GetPathChainBolt's output is a HashMap, otherwise this will
        //cause a ClassCastException
        //key is 'O', value is 'D'
        HashMap<PathInfoNode, PathInfoNode>
                od = ((ODListNode) tuple.getValueByField("single-car-od-list")).getOdHashmap();
        _collector.ack(tuple);

        //not exist insert otherwise update
        String sqlString = "INSERT INTO single_car_od(EID,od_O_ip,od_D_ip,"
                + "od_O_time,od_D_time,car_type) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE EID=?";
        try {
            pre = conn.prepareStatement(sqlString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //traverse od map to calc single car's od.
        for (Map.Entry<PathInfoNode, PathInfoNode> node : od.entrySet()) {
            PathInfoNode key = node.getKey();
            PathInfoNode value = node.getValue();

            //exclude OD like 'AA' in which 'A' is both 'O' and 'D'
            // if (!key.getIp().equals(value.getIp())) {
            try {
                pre.setString(1, eid);
                pre.setString(2, key.getIp());
                pre.setString(3, value.getIp());
                pre.setTimestamp(4, new Timestamp(key.getPassTime().getTime()));
                pre.setTimestamp(5, new Timestamp(value.getPassTime().getTime()));
                pre.setString(6, key.getCar_type());
                pre.setString(7, eid);
                //improve efficiency.
                pre.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            //  }
        }
        try {
            pre.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        //close connection to database after killing topology.
        try {
            if (pre != null)
                pre.close();
            if (conn != null)
                conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
