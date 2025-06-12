import java.sql.*;

public class CouponCounter {
    static void check(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            // 총 건수
            ResultSet totalRs = stmt.executeQuery("SELECT COUNT(*) FROM BONUS_COUPON");
            if (totalRs.next()) {
                System.out.println("📦 BONUS_COUPON 총 건수: " + totalRs.getInt(1));
            }
            totalRs.close();

            // 쿠폰 코드별 분포
            ResultSet groupRs = stmt.executeQuery(
                    "SELECT COUPON_CD, COUNT(*) FROM BONUS_COUPON GROUP BY COUPON_CD ORDER BY COUPON_CD"
            );
            System.out.println("📊 쿠폰 코드별 건수:");
            while (groupRs.next()) {
                String code = groupRs.getString(1);
                int cnt = groupRs.getInt(2);
                System.out.printf("- %s: %d건\n", code, cnt);
            }
            groupRs.close();

        } catch (SQLException e) {
            System.err.println("[ERROR] 검증 쿼리 실행 실패:");
            e.printStackTrace();
        }
    }
}

