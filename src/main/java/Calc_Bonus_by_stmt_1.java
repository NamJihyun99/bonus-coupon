import java.sql.*;
import java.time.LocalDate;

public class Calc_Bonus_by_stmt_1 {

    static void run() {
        int count = 0;

        try (Connection conn = DBConnectionUtil.getNewConnection()) {

            // 1. 전체 고객 데이터 조회
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(10);
            ResultSet rs = stmt.executeQuery(
                    "SELECT ID, EMAIL, ENROLL_DT, CREDIT_LIMIT, GENDER, ADDRESS1 FROM CUSTOMER "
            );

            // 2. 고객 데이터 필터링 및 BONUS_COUPON Insert
            String yyyymm = "202506";

            LocalDate baseDate = LocalDate.of(2018, 1, 1);

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

                // Insert
                String insertSQL = String.format(
                        "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                                "VALUES ('%s', '%s', '%s', '%s', %d, NULL, NULL, NULL)",
                        yyyymm, customerId, email, couponCd, credit
                );

                try (Statement insertStmt = conn.createStatement()) {
                    insertStmt.executeUpdate(insertSQL);
                    count++;
                }
            }
            rs.close();
            stmt.close();

            CouponUtil.countInsertion(conn);
        } catch (SQLException e) {
            System.err.println("[ERROR] insert count = " + count);
            e.printStackTrace();
        }
    }
}
