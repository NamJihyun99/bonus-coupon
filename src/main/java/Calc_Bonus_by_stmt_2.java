import java.sql.*;
import java.text.SimpleDateFormat;

public class Calc_Bonus_by_stmt_2 {

    static void run() {

        try (Connection conn = DBConnectionUtil.getNewConnection()) {

            // 1. 초기화 - BONUS_COUPON 테이블 비우기
            try (Statement truncateStmt = conn.createStatement()) {
                truncateStmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");
            }

            // 2. 고객 데이터 조회
            Statement selectStmt = conn.createStatement();
            selectStmt.setFetchSize(10);
            ResultSet rs = selectStmt.executeQuery(
                    "SELECT CUSTOMER_ID, EMAIL, ENROLL_DT, CREDIT_LIMIT, GENDER, ADDR " +
                            "FROM CUSTOMER WHERE ENROLL_DT >= TO_DATE('20180101', 'YYYYMMDD')"
            );

            // 3. insert용 Statement 단 1회 생성
            Statement insertStmt = conn.createStatement();

            int count = 0;
            String yyyymm = "202506";
            String sendDate = "2025-06-25";
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String receiveDt = sdf.format(Date.valueOf("2025-06-25"));

            while (rs.next()) {
                String customerId = rs.getString("CUSTOMER_ID");
                String email = rs.getString("EMAIL");
                int credit = rs.getInt("CREDIT_LIMIT");
                String gender = rs.getString("GENDER");
                String addr = rs.getString("ADDR");
                String couponCd;

                // 4. 쿠폰 코드 계산
                if (credit < 1000) {
                    couponCd = "AA";
                } else if (credit <= 2999) {
                    couponCd = "BB";
                } else if (credit <= 3999) {
                    if ("F".equalsIgnoreCase(gender) && addr.contains("송파구") && addr.contains("풍납1동")) {
                        couponCd = "C2";
                    } else {
                        couponCd = "CC";
                    }
                } else {
                    couponCd = "DD";
                }

                // 5. INSERT SQL 생성
                String insertSQL = String.format(
                        "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                                "VALUES ('%s', '%s', '%s', '%s', %d, TO_DATE('%s','YYYY-MM-DD'), '%s', NULL)",
                        yyyymm, customerId, email, couponCd, credit, sendDate, receiveDt
                );

                try {
                    insertStmt.executeUpdate(insertSQL);
                    count++;
                } catch (SQLException insertEx) {
                    System.err.println("[ERROR] Insert failed for customer_id: " + customerId);
                    insertEx.printStackTrace(); // 로그 출력
                    // rollback 불필요, 다음 고객 계속 처리
                }
            }

            // 리소스 반납
            rs.close();
            selectStmt.close();
            insertStmt.close();

            System.out.println("총 발송 수: " + count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
