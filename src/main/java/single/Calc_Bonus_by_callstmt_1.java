package single;

import common.CouponUtil;
import common.DBConnectionUtil;

import java.sql.*;

public class Calc_Bonus_by_callstmt_1 {
    public static void run() {

        int count = 0;
        try (Connection conn = DBConnectionUtil.getNewConnection()) {
            // 1. BONUS_COUPON 테이블 초기화
            CouponUtil.truncate(conn);

            // 2. Anonymous PL/SQL Block
            CallableStatement cstmt = conn.prepareCall(
                    "DECLARE\n" +
                            "    CURSOR cur IS\n" +
                            "        SELECT ID, EMAIL, ENROLL_DT, CREDIT_LIMIT, GENDER, ADDRESS1\n" +
                            "        FROM CUSTOMER;\n" +
                            "\n" +
                            "    v_count NUMBER := 0;\n" +
                            "BEGIN\n" +
                            "    FOR rec IN cur LOOP\n" +
                            "        IF rec.ENROLL_DT >= TO_DATE('20130101', 'YYYYMMDD') THEN\n" +
                            "            DECLARE\n" +
                            "                v_coupon_cd CHAR(2);\n" +
                            "            BEGIN\n" +
                            "                IF rec.CREDIT_LIMIT < 1000 THEN\n" +
                            "                    v_coupon_cd := 'AA';\n" +
                            "                ELSIF rec.CREDIT_LIMIT BETWEEN 1000 AND 2999 THEN\n" +
                            "                    v_coupon_cd := 'BB';\n" +
                            "                ELSIF rec.CREDIT_LIMIT BETWEEN 3000 AND 3999 THEN\n" +
                            "                    IF rec.GENDER = 'F' AND rec.ADDRESS1 LIKE '%송파구%' AND rec.ADDRESS1 LIKE '%풍납1동%' THEN\n" +
                            "                        v_coupon_cd := 'C2';\n" +
                            "                    ELSE\n" +
                            "                        v_coupon_cd := 'CC';\n" +
                            "                    END IF;\n" +
                            "                ELSE\n" +
                            "                    v_coupon_cd := 'DD';\n" +
                            "                END IF;\n" +
                            "\n" +
                            "                INSERT INTO BONUS_COUPON (\n" +
                            "                    YYYYMM, CUSTOMER_ID, EMAIL, COUPON_CD,\n" +
                            "                    CREDIT_POINT, SEND_DT, RECEIVE_DT, USE_DT\n" +
                            "                ) VALUES (\n" +
                            "                    '202506', rec.ID, rec.EMAIL, v_coupon_cd,\n" +
                            "                    rec.CREDIT_LIMIT, NULL, NULL, NULL\n" +
                            "                );\n" +
                            "\n" +
                            "                v_count := v_count + 1;\n" +
                            "                IF MOD(v_count, 10000) = 0 THEN\n" +
                            "                    COMMIT;\n" +
                            "                END IF;\n" +
                            "            END;\n" +
                            "        END IF;\n" +
                            "    END LOOP;\n" +
                            "    COMMIT;\n" +
                            "END;"
            );

            cstmt.execute();
            cstmt.close();

            // 결과 출력
            CouponUtil.countInsertion(conn);
        } catch (SQLException e) {
            System.err.println("[ERROR] 발송 건수: " + count);
            e.printStackTrace();
        }
    }
}
