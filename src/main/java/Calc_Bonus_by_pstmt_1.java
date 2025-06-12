import java.sql.*;
import java.text.SimpleDateFormat;

public class Calc_Bonus_by_pstmt_1 {

    public static void run() {
        try (Connection conn = DBConnectionUtil.getNewConnection()) {
            conn.setAutoCommit(false);

            // 1. BONUS_COUPON 테이블 초기화
            try (Statement truncateStmt = conn.createStatement()) {
                truncateStmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");
                conn.commit();
            }

            // 2. insert용 PreparedStatement 1회 생성 (Hard Parsing 개선)
            String insertSQL = "INSERT INTO BONUS_COUPON " +
                    "(YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                    "VALUES (?, ?, ?, ?, ?, TO_DATE(?, 'YYYY-MM-DD'), ?, NULL)";
            PreparedStatement insertPstmt = conn.prepareStatement(insertSQL);

            // 3. 지급 대상 SELECT + fetch size 조정
            Statement selectStmt = conn.createStatement();
            selectStmt.setFetchSize(1000);
            ResultSet rs = selectStmt.executeQuery(
                    "SELECT CUSTOMER_ID, EMAIL, CREDIT_LIMIT, GENDER, ADDR " +
                            "FROM CUSTOMER " +
                            "WHERE ENROLL_DT >= TO_DATE('20180101', 'YYYYMMDD') " +
                            "AND CREDIT_LIMIT IS NOT NULL"
            );

            // 공통 필드
            String yyyymm = "202506";
            String sendDate = "2025-06-25";
            String receiveDt = new SimpleDateFormat("yyyyMMdd").format(Date.valueOf("2025-06-25"));

            int count = 0;
            int batchCount = 0;

            while (rs.next()) {
                String customerId = rs.getString("CUSTOMER_ID");
                String email = rs.getString("EMAIL");
                int credit = rs.getInt("CREDIT_LIMIT");
                String gender = rs.getString("GENDER");
                String addr = rs.getString("ADDR");
                String couponCd = null;

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

                if (couponCd == null) continue;

                try {
                    insertPstmt.setString(1, yyyymm);
                    insertPstmt.setString(2, customerId);
                    insertPstmt.setString(3, email);
                    insertPstmt.setString(4, couponCd);
                    insertPstmt.setInt(5, credit);
                    insertPstmt.setString(6, sendDate);
                    insertPstmt.setString(7, receiveDt);

                    insertPstmt.executeUpdate();
                    count++;
                    batchCount++;

                    if (batchCount >= 10_000) {
                        conn.commit();
                        batchCount = 0;
                    }
                } catch (SQLException ex) {
                    System.err.println("[ERROR] Insert failed for customer_id: " + customerId);
                    ex.printStackTrace();
                }
            }

            // 마지막 잔여 커밋
            if (batchCount > 0) {
                conn.commit();
            }

            // 리소스 정리
            rs.close();
            selectStmt.close();
            insertPstmt.close();

            // 결과 출력
            System.out.println("총 발송 건수: " + count);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
