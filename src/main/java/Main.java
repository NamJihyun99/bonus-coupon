import java.sql.Connection;

public class Main {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

//         Calc_Bonus_by_callstmt_1.run();
        Calc_Bonus_by_pstmt_1.run();
//         Calc_Bonus_by_stmt_1.run();

        long end = System.currentTimeMillis();
        System.out.println("총 처리 시간(ms): " + (end - start));
    }
}
