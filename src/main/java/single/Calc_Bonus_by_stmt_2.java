package single;

import common.*;

import java.sql.*;
import java.time.LocalDate;

public class Calc_Bonus_by_stmt_2 {

    static void run() {

        System.out.println("Calc_Bonus_by_stmt_2");
        int count = 0;

        try (Connection conn = DBConnectionUtil.getNewConnection()) {

            // 1. 테이블 초기화
            CouponUtil.truncate(conn);

            // 2. 고객 데이터 조회
            Statement selectStmt = conn.createStatement();
            selectStmt.setFetchSize(10);
            ResultSet rs = selectStmt.executeQuery(
                    "SELECT ID, EMAIL, ENROLL_DT, CREDIT_LIMIT, GENDER, ADDRESS1 FROM CUSTOMER"
            );

            // 3. insert용 Statement 단 1회 생성
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

                // 4. INSERT SQL 생성ADDR
                String insertSQL = String.format(
                        "INSERT INTO BONUS_COUPON (YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD, CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT) " +
                                "VALUES ('%s', '%s', '%s', '%s', %d, NULL, NULL, NULL)",
                        yyyymm, customerId, email, couponCd, credit
                );

                // INSERT statement 재사용
                // auto commit : statement 실행마다 commit
                insertStmt.executeUpdate(insertSQL);
                count++;
            }

            // 리소스 반납
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
