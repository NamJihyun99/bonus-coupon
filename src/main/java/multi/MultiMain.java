package multi;

import common.*;

import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiMain {

    static final int THREAD_COUNT = 7;

    public static void main(String[] args) {
        try (ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT)) {

            try (Connection conn = DBConnectionUtil.getNewConnection()) {
                CouponUtil.truncate(conn);  // 초기화는 메인 커넥션으로 한 번만 수행

                // int totalRows = common.CouponUtil.countAll(conn);
                int totalRows = CouponUtil.countCondition(conn);
                int pageSize = (int) Math.ceil((double) totalRows / THREAD_COUNT);

                long start = System.currentTimeMillis();

                for (int i = 0; i < THREAD_COUNT; i++) {
                    final int top = i * pageSize + 1;
                    final int bottom = Math.min((i + 1) * pageSize, totalRows);

                    executor.submit(() -> {
                        try (Connection threadConn = DBConnectionUtil.getNewConnection()) {
                            Multi_Calc_Bonus_by_stmt_2.run(threadConn, top, bottom);
                        } catch (Exception e) {
                            System.err.printf("[THREAD %s] 커넥션 생성 또는 작업 중 오류%n", Thread.currentThread().getName());
                            e.printStackTrace();
                        }
                    });
                }

                executor.shutdown();
                executor.awaitTermination(2, TimeUnit.HOURS);

                long end = System.currentTimeMillis();
                System.out.println("총 처리 시간(ms): " + (end - start));

                CouponUtil.countInsertion(conn); // 검증도 메인 커넥션으로

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
