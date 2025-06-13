import java.sql.*;

public class Multi_Calc_Bonus_by_stmt_3 {

    public static void run(Connection conn, int startRow, int endRow) {
        int count = 0;

        try {
            // 지급 대상만 포함하는 조건 + ROWNUM으로 페이징
            Statement selectStmt = conn.createStatement();
            selectStmt.setFetchSize(10);
            ResultSet rs = selectStmt.executeQuery(
                    String.format(
                            "SELECT * FROM (SELECT ROWNUM AS RN, C.* " +
                                    "FROM (SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1 " +
                                    "      FROM CUSTOMER " +
                                    "      WHERE ENROLL_DT >= TO_DATE('20130101', 'YYYYMMDD')) C) " +
                                    "WHERE RN BETWEEN %d AND %d",
                            startRow, endRow
                    )
            );

            Statement insertStmt = conn.createStatement();
            String yyyymm = "202506";

            while (rs.next()) {
                String customerId = rs.getString("ID");
                String email = rs.getString("EMAIL");
                int credit = rs.getInt("CREDIT_LIMIT");
                String gender = rs.getString("GENDER");
                String addr = rs.getString("ADDRESS1");

                String couponCd = Coupon.getCode(credit, gender, addr);

                String insertSQL = String.format(
                        "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                                "VALUES ('%s', '%s', '%s', '%s', %d, NULL, NULL, NULL)",
                        yyyymm, customerId, email, couponCd, credit
                );

                insertStmt.executeUpdate(insertSQL);
                count++;
            }

            rs.close();
            selectStmt.close();
            insertStmt.close();

            System.out.printf("[THREAD %s] 처리 완료 - %d건%n", Thread.currentThread().getName(), count);

        } catch (SQLException e) {
            System.err.printf("[THREAD %s] 오류 발생. insert count = %d%n", Thread.currentThread().getName(), count);
            e.printStackTrace();
        }
    }
}

