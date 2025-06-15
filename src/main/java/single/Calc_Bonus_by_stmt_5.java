package single;

import common.*;

import java.sql.*;

public class Calc_Bonus_by_stmt_5 {

    static void run() {
        int count = 0;

        try (Connection conn = DBConnectionUtil.getNewConnection()) {
            // auto commit 해제
            conn.setAutoCommit(false);

            // 1. BONUS_COUPON 테이블 초기화
            CouponUtil.truncate(conn);

            // 2. insert용 Statement 재사용
            Statement insertStmt = conn.createStatement();

            // 3. 지급 대상 고객만 SELECT
            Statement selectStmt = conn.createStatement();

            // fetch size : 1000
            selectStmt.setFetchSize(1000);
            ResultSet rs = selectStmt.executeQuery(
                    "SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1 " +
                            "FROM CUSTOMER " +
                            "WHERE ENROLL_DT >= TO_DATE('20130101', 'YYYYMMDD') "
            );

            String yyyymm = "202506";

            int batchCount = 0;

            while (rs.next()) {
                String customerId = rs.getString("ID");
                String email = rs.getString("EMAIL");
                int credit = rs.getInt("CREDIT_LIMIT");
                String gender = rs.getString("GENDER");
                String addr = rs.getString("ADDRESS1");
                String couponCd = Coupon.getCode(credit, gender, addr);

                String insertSQL = String.format(
                        "INSERT INTO BONUS_COUPON " +
                                "(YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                                "VALUES ('%s', '%s', '%s', '%s', %d, NULL, NULL, NULL)",
                        yyyymm, customerId, email, couponCd, credit
                );

                insertStmt.executeUpdate(insertSQL);
                count++;
                batchCount++;

                // 10,000건마다 commit
                if (batchCount >= 10_000) {
                    conn.commit();
                    batchCount = 0;
                }
            }

            // 잔여 커밋 처리
            if (batchCount > 0) {
                conn.commit();
            }

            // 리소스 정리
            rs.close();
            selectStmt.close();
            insertStmt.close();

            // 결과 출력
            CouponUtil.countInsertion(conn);
        } catch (SQLException e) {
            System.err.println("[ERROR] 발송 건수: " + count);
            e.printStackTrace();
        }
    }
}
