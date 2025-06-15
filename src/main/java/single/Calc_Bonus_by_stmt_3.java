package single;

import common.*;

import java.sql.*;

public class Calc_Bonus_by_stmt_3 {

    static void run() {

        int count = 0;
        try (Connection conn = DBConnectionUtil.getNewConnection()) {

            // 1. 테이블 초기화
            CouponUtil.truncate(conn);

            // 2. insert용 Statement는 1회만 생성하여 재사용
            Statement insertStmt = conn.createStatement();

            // 3. 지급 대상만 SELECT (WHERE 조건 포함)
            Statement selectStmt = conn.createStatement();
            selectStmt.setFetchSize(10);
            ResultSet rs = selectStmt.executeQuery(
                    "SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1 " +
                            "FROM CUSTOMER " +
                            "WHERE ENROLL_DT >= TO_DATE('20130101', 'YYYYMMDD') "
            );

            String yyyymm = "202506";

            // 5. 로직 수행
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

                // auto commit : statement 실행마다 commit
                insertStmt.executeUpdate(insertSQL);
                count++;
            }

            // 6. 자원 정리 및 종료 처리
            rs.close();
            selectStmt.close();
            insertStmt.close();

            CouponUtil.countInsertion(conn);
        } catch (SQLException e) {
            System.err.println("[ERROR] 발송 건수: " + count);
            e.printStackTrace();
        }
    }
}
