import java.sql.*;
import java.time.LocalDate;

public class Multi_Calc_Bonus_by_stmt_2 {

    public static void run(Connection conn, int startRow, int endRow) {
        int count = 0;

        try {
            Statement selectStmt = conn.createStatement();
            selectStmt.setFetchSize(10);

            // 페이징 처리된 범위 조회
            ResultSet rs = selectStmt.executeQuery(
                    String.format(
                            "SELECT * FROM (SELECT ROWNUM AS RN, C.* FROM CUSTOMER C) " +
                                    "WHERE RN BETWEEN %d AND %d",
                            startRow, endRow
                    )
            );

            // insert용 Statement 단 1회 생성
            Statement insertStmt = conn.createStatement();

            String yyyymm = "202506";
            LocalDate baseDate = LocalDate.of(2013, 1, 1);

            while (rs.next()) {
                Date enrollDt = rs.getDate("ENROLL_DT");
                if (enrollDt == null || enrollDt.toLocalDate().isBefore(baseDate)) {
                    continue;
                }

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
