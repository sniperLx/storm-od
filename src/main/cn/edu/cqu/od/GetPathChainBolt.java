package cn.edu.cqu.od;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Created by lab on 2016/4/29.
 *
 * @author lx
 */
public class GetPathChainBolt implements IRichBolt {
    private static final long serialVersionUID = -9197527519514789462L;

    private static Logger logger = LoggerFactory.getLogger(GetPathChainBolt.class);

    private Connection conn = null;
    private PreparedStatement pre = null;
    private ResultSet resultSet = null;

    private OutputCollector _collector;
    private List<Integer> numCounterTasts = null;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declare two stream, one ordinary stream flow to 'ODCalcBolt',
        // one direct stream flow to 'SaveONPEIDBolt'
        declarer.declare(new Fields("eid", "single-car-path-chain"));
        declarer.declareStream("oneNodePath", true, new Fields("eid"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
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
        _collector = collector;
        numCounterTasts = context.getComponentTasks("save-onpEID-bolt");
    }


    public void execute(Tuple input) {
        indexed_send(input);
    }

    private void indexed_send(Tuple input) {

        //kafkaSpout output a string named "str".
        String eid = input.getStringByField("str");
        logger.info("dealing with " + eid);

        //store the path chain in a ArrayList.
        ArrayList<PathInfoNode> path_chain = new ArrayList<PathInfoNode>();

        //get this eid's path chain from db
        String sql = "SELECT rfid_ip, EID, Passtime, car_type" +
                " FROM rfid WHERE EID = ?";
        try {
            pre = conn.prepareStatement(sql);
            pre.setString(1, eid);
            //avoid java.sql.SQLException: java.lang.OutOfMemoryError: Java heap space
            //pre.setFetchSize(Integer.MIN_VALUE);
            resultSet = pre.executeQuery();

            while (resultSet.next()) {
                String ip = resultSet.getString("rfid_ip");
                Date time = new java.util.Date(resultSet.getTimestamp("Passtime").getTime());
                String car_type = resultSet.getString("car_type");
                PathInfoNode node = new PathInfoNode(ip, time, car_type);
                path_chain.add(node);
            }
            if (path_chain.size() > 1) {
                //sort the path chain by date
                sortByDate(path_chain);
                List<Object> tuple = new Values(eid, path_chain);
                _collector.emit(input, tuple);
                _collector.ack(input);
            } else {
                /**
                 * If the size of path chain is one or smaller, then emit its eid into
                 * the direct stream "oneNodePath" and SaveONPEIDBolt will read from it.
                 */
                List<Object> tuple = new Values(eid);
                _collector.emitDirect(setTastId(), "oneNodePath", input, tuple);
                _collector.ack(input);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Integer setTastId() {
        int len = numCounterTasts.size();
        Random random = new Random();
        int index = random.nextInt(len);
        return numCounterTasts.get(index);
    }

    private void sortByDate(ArrayList<PathInfoNode> path_chain) {
        Comparator<PathInfoNode> comparator = new Comparator<PathInfoNode>() {
            public int compare(PathInfoNode o1, PathInfoNode o2) {
                return o1.getPassTime().compareTo(o2.getPassTime());
            }
        };
        Collections.sort(path_chain, comparator);
    }

    public void cleanup() {
        try {
            if (resultSet != null)
                resultSet.close();
            if (pre != null)
                pre.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
