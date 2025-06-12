import java.sql.*;
import java.text.SimpleDateFormat;

public class Calc_Bonus_by_stmt_3 {

    static void run() {
        long start = System.currentTimeMillis();

        try (Connection conn = DBConnectionUtil.getNewConnection()) {

            // 1. 테이블 초기화
            try (Statement truncateStmt = conn.createStatement()) {
                truncateStmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");
            }

            // 2. insert용 Statement는 1회만 생성하여 재사용
            Statement insertStmt = conn.createStatement();

            // 3. 지급 대상만 SELECT (WHERE 조건 포함)
            Statement selectStmt = conn.createStatement();
            selectStmt.setFetchSize(10); // 메모리 부담 완화
            ResultSet rs = selectStmt.executeQuery(
                    "SELECT CUSTOMER_ID, EMAIL, CREDIT_LIMIT, GENDER, ADDR " +
                            "FROM CUSTOMER " +
                            "WHERE ENROLL_DT >= TO_DATE('20180101', 'YYYYMMDD') " +
                            "AND CREDIT_LIMIT IS NOT NULL"
            );

            // 4. 공통 변수 정의
            String yyyymm = "202506";
            String sendDate = "2025-06-25";
            String receiveDt = new SimpleDateFormat("yyyyMMdd").format(Date.valueOf("2025-06-25"));

            int count = 0;

            // 5. 로직 수행
            while (rs.next()) {
                String customerId = rs.getString("CUSTOMER_ID");
                String email = rs.getString("EMAIL");
                int credit = rs.getInt("CREDIT_LIMIT");
                String gender = rs.getString("GENDER");
                String addr = rs.getString("ADDR");
                String couponCd;

                // 쿠폰 코드 결정
                if (credit < 1000) {
                    couponCd = "AA";
                } else if (credit <= 2999) {
                    couponCd = "BB";
                } else if (credit <= 3999) {
                    if ("F".equalsIgnoreCase(gender) &&
                            addr != null &&
                            addr.contains("송파구") &&
                            addr.contains("풍납1동")) {
                        couponCd = "C2";
                    } else {
                        couponCd = "CC";
                    }
                } else {
                    couponCd = "DD";
                }

                String insertSQL = String.format(
                        "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                                "VALUES ('%s', '%s', '%s', '%s', %d, TO_DATE('%s', 'YYYY-MM-DD'), '%s', NULL)",
                        yyyymm, customerId, email, couponCd, credit, sendDate, receiveDt
                );

                try {
                    insertStmt.executeUpdate(insertSQL);
                    count++;
                } catch (SQLException ex) {
                    System.err.println("[ERROR] Insert failed for customer_id: " + customerId);
                    ex.printStackTrace();
                }
            }

            // 6. 자원 정리 및 종료 처리
            rs.close();
            selectStmt.close();
            insertStmt.close();

            System.out.println("총 발송 건수: " + count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
