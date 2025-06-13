import java.sql.*;
import java.text.SimpleDateFormat;

public class Calc_Bonus_by_pstmt_2 {
    public static void run() {
        int count = 0;
        try (Connection conn = DBConnectionUtil.getNewConnection()) {
            conn.setAutoCommit(false);

            // 1. BONUS_COUPON 테이블 초기화
            CouponUtil.truncate(conn);

            // 2. insert용 PreparedStatement 생성
            String insertSQL = "INSERT INTO BONUS_COUPON " +
                    "(YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                    "VALUES (?, ?, ?, ?, ?,NULL, NULL, NULL)";
            PreparedStatement insertPstmt = conn.prepareStatement(insertSQL);

            // 3. SELECT 지급 대상 + fetch size 조정
            Statement selectStmt = conn.createStatement();
            selectStmt.setFetchSize(1000);
            ResultSet rs = selectStmt.executeQuery(
                    "SELECT ID, EMAIL, CREDIT_LIMIT, GENDER, ADDRESS1 " +
                            "FROM CUSTOMER " +
                            "WHERE ENROLL_DT >= TO_DATE('20180101', 'YYYYMMDD') "
            );

            String yyyymm = "202506";

            int batchCount = 0;
            int commitCount = 0;

            while (rs.next()) {
                String customerId = rs.getString("ID");
                String email = rs.getString("EMAIL");
                int credit = rs.getInt("CREDIT_LIMIT");
                String gender = rs.getString("GENDER");
                String addr = rs.getString("ADDRESS1");
                String couponCd = Coupon.getCode(credit, gender, addr);

               insertPstmt.setString(1, yyyymm);
               insertPstmt.setString(2, customerId);
               insertPstmt.setString(3, email);
               insertPstmt.setString(4, couponCd);
               insertPstmt.setInt(5, credit);

               insertPstmt.addBatch();
               count++;
               batchCount++;

               // 1,000건마다 executeBatch()
                if (batchCount >= 1000) {
                    insertPstmt.executeBatch();
                    batchCount = 0;
                }

                // 10,000건마다 commit()
                if (count % 10_000 == 0) {
                    conn.commit();
                }
            }

            // 남은 batch 처리
            if (batchCount > 0) {
                insertPstmt.executeBatch();
            }

            // 잔여 커밋
            conn.commit();

            // 리소스 정리
            rs.close();
            selectStmt.close();
            insertPstmt.close();

            // 결과 출력
            CouponUtil.countInsertion(conn);
        } catch (SQLException e) {
            System.err.println("[ERROR] 발송 건수: " + count);
            e.printStackTrace();
        }
    }
}
