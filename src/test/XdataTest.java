import java.sql.*;

/**
 * Created by lab on 2016/5/16.
 */
public class XdataTest {
    public static void main(String[] args) {
        String driver = "com.sugon.xdata.jdbc.XDataDriver";
        Connection conn = null;
        String sql = "SELECT * from rfid1 where rownum<10";
        try{
            Class.forName(driver);
            conn = DriverManager.getConnection("./src/client.ini","dba","dba123");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while(rs.next()) {
                System.out.println(rs.getInt(1));
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {

                try {
                    if(conn != null)
                        conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
    }
}
