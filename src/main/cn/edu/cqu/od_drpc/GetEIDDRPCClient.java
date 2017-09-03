package cn.edu.cqu.od_drpc;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by lab on 2016/4/26.
 *
 * @author lx
 */

//read single car's path chain into memory.
//which has been sorted by 'time' field in db.
public class GetEIDDRPCClient {
    public static final Logger LOG = LoggerFactory.getLogger(GetEIDDRPCClient.class);

    private final static int queueSize = 1024000;
    private static LinkedBlockingDeque<String> eid_queue = new LinkedBlockingDeque<String>(queueSize);

    Connection conn = null;
    PreparedStatement pre = null;
    static ResultSet resultSet = null;

    private Thread producer = null;

    final String host_port = "node1_IP:3306";
    final String database = "od_week";
    final String user = "lx";
    final String password = "password_mysql";
    final String url = "jdbc:mysql://" + host_port + "/" + database;

    private void getEid() {
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
        /*producer = new Thread(new Runnable() {
            public void run() {
                try {
                    while (resultSet.next()) {
                        System.out.println("hello in thread");
                        String eid = resultSet.getString("EID");
                        System.out.println(eid);
                        //control output speed
                        //Utils.sleep(4);
                        eid_queue.put(eid);
                    }
                } catch (SQLException e) {
                    System.err.println("error from client");
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        producer.start();
        System.err.println("hello from open in getEIDDRPCClient");*/
    }

    public static void main(String[] args) {
//        String drpc_server = "10.0.0.1";
        String drpc_server = "222.198.138.113";
        int port = 3772;
        DRPCClient client = new DRPCClient(drpc_server, port);
        GetEIDDRPCClient getEIDClient = new GetEIDDRPCClient();
        getEIDClient.getEid();
        String eid = null;
       /* while ((eid = eid_queue.poll()) != null) {
            try {
                String status = client.execute("od-drpc",eid);
                System.out.println(status);
            } catch (TException e) {
                e.printStackTrace();
            } catch (DRPCExecutionException e) {
                e.printStackTrace();
            }
        }*/
        int i = 0;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//�������ڸ�ʽ
        try {
            while (resultSet.next()) {
                eid = resultSet.getString("EID");
                try {
//                    System.err.print(eid);
                    String status = client.execute("od-drpc", eid);
                    //LOG.info(df.format(new Date(System.currentTimeMillis())) + "=====>" + status);
                    LOG.info("=====>" + status);
                } catch (TException e) {
                    e.printStackTrace();
                } catch (DRPCExecutionException e) {
                    e.printStackTrace();
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        getEIDClient.close();

        /*String status = null;
        eid = "2548149";
        try {
            status = client.execute("od-drpc",eid);
        } catch (TException e) {
            e.printStackTrace();
        } catch (DRPCExecutionException e) {
            e.printStackTrace();
        }
        //System.out.println(eid + "=====>" + status);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//�������ڸ�ʽ
        //System.out.println(df.format(new Date()));// new Date()Ϊ��ȡ��ǰϵͳʱ��
        LOG.info(df.format(new Date(System.currentTimeMillis())) + "=====>" + status);*/

    }

    void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (pre != null) {
                pre.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            producer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
