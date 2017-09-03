package cn.edu.cqu.od_Test;

import java.io.*;
import java.sql.*;

/**
 * Created by lab on 2016/4/29.
 */
public class DemoOD {
    Connection conn = null;
    PreparedStatement pre = null;
    PreparedStatement pre1 = null;
    ResultSet resultSet = null;

    void linkDB() {
        // final String host_port = "172.24.188.235:3306";
        final String host_port = "222.198.138.113:3306";
        final String database = "od_week";
        final String user = "lx";
        final String password = "3124769";
        final String url = "jdbc:mysql://" + host_port + "/" + database;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final String sql = "SELECT DISTINCT eid FROM rfid";
        try {
            pre = conn.prepareStatement(sql);
            resultSet = pre.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void closeDB() {
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

    void readEID() {
        FileWriter fw = null;
        try {
            fw = new FileWriter(new File("./src/test/eid-week"), true);
            int i = 0;
            while (resultSet.next()) {
                String eid = resultSet.getString("EID");
                fw.write(eid);
                fw.write("\n");
                // if(i++ > 10000) break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fw != null)
                    fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void writeEIDIntoDB() {
        Thread thread1 = new Thread(new Runnable() {
            public void run() {
                try {
                    String sql = "INSERT INTO eid(EID) VALUES(?)";
                    conn.setAutoCommit(false);
                    pre1 = conn.prepareStatement(sql);
                    int i = 1;
                    String eid = null;
                    FileReader fileReader = new FileReader(new File("./src/test/eid-week"));
                    BufferedReader bf = new BufferedReader(fileReader);
                    while ((eid = bf.readLine()) != null) {
                        //eid = resultSet.getString("EID");
                        pre1.setString(1, eid);
                        pre1.addBatch();
                        //pre1.executeUpdate();
                        //System.err.println(eid);
                        if (i++ >= 5000) {
                            pre1.executeBatch();
                            conn.commit();
                            i = 1;
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        pre1.executeBatch();
                        conn.commit();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                }
            }
        });
        thread1.start();
        try {
            thread1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    boolean check_exist(String eid) {
        String sql = "select count(eid) from single_car_od where eid = ?";
        Integer count = -1;
        try {
            pre = conn.prepareStatement(sql);
            pre.setString(1, eid);
            ResultSet res = pre.executeQuery();
            if (res.next()) {
                count = res.getInt(1);
                System.err.println("in getEIDspout: " + count);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return !(count == 0);
    }

    public static void main(String[] args) {
        DemoOD demoOD = new DemoOD();
        demoOD.linkDB();
        //demoOD.readEID();
        demoOD.writeEIDIntoDB();
        //String eid = "1000391";
        //demoOD.check_exist(eid);
        demoOD.closeDB();
        //System.out.println(args[0]);

    }
}
