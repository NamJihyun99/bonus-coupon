import java.sql.*;

public class CouponUtil {

    static void countInsertion(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            // 총 건수
            ResultSet totalRs = stmt.executeQuery("SELECT COUNT(*) FROM BONUS_COUPON");
            if (totalRs.next()) {
                System.out.println(" BONUS_COUPON 총 건수: " + totalRs.getInt(1));
            }
            totalRs.close();

            // 쿠폰 코드별 분포
            ResultSet groupRs = stmt.executeQuery(
                    "SELECT COUPON_CD, COUNT(*) FROM BONUS_COUPON GROUP BY COUPON_CD ORDER BY COUPON_CD"
            );
            System.out.println(" 쿠폰 코드별 건수:");
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

    static int countAll(Connection conn) {
        String sql = "SELECT COUNT(*) FROM CUSTOMER";
        int total = 0;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                total = rs.getInt(1);
                System.out.printf("CUSTOMER 총 건수: %,d건%n", total);
            }

        } catch (SQLException e) {
            System.err.println("[ERROR] CUSTOMER 전체 건수 조회 실패");
            e.printStackTrace();
        }

        return total;
    }

    static int countCondition(Connection conn) {
        String sql = "SELECT COUNT(*) FROM CUSTOMER WHERE ENROLL_DT >= TO_DATE('20180101', 'YYYYMMDD')";
        int total = 0;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                total = rs.getInt(1);
                System.out.printf("CUSTOMER 총 건수: %,d건%n", total);
            }

        } catch (SQLException e) {
            System.err.println("[ERROR] CUSTOMER 전체 건수 조회 실패");
            e.printStackTrace();
        }

        return total;
    }

    static void truncate(Connection conn) throws SQLException {
        try (Statement truncateStmt = conn.createStatement()) {
            truncateStmt.executeUpdate("TRUNCATE TABLE BONUS_COUPON");
        }
    }
}

