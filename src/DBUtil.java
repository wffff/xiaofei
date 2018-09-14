import java.sql.*;

public class DBUtil {
    private static String dirver = "org.postgresql.Driver";
    private static String url = "jdbc:postgresql://47.96.92.163:5432/sncj";
    private static String user = "postgres";
    private static String password = "123";
    private static Connection con;
    private static Statement stat;
    private static ResultSet rs;
    public static void insetData(String data){
        try {
            Class.forName(dirver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String sql = "INSERT INTO t_message (message) VALUES ('"+data+"')";
        try{
            con = DriverManager.getConnection(url, user, password);
            stat = con.createStatement();
            System.out.println("影响行数"+stat.executeUpdate(sql));
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}