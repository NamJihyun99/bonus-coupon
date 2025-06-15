package common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnectionUtil {

    private static final String DB_URL = "jdbc:oracle:thin:@192.168.119.119:1521/dinkdb";
    private static final String DB_USER = "scott";
    private static final String DB_PASSWORD = "tiger";

//    private static final String DB_URL = "jdbc:oracle:thin:@192.168.217.206:1521/KOPODA";
//    private static final String DB_USER = "da2502";
//    private static final String DB_PASSWORD = "da02";

    public static Connection getNewConnection() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Oracle 드라이버 로딩 실패 : " + e.getMessage());
        } catch (SQLException e) {
            throw new RuntimeException("DB 연결에 실패했습니다 : " + e.getMessage());
        }
    }
}
