public class Coupon {

    public static String getCode(int credit, String gender, String addr) {
        // 쿠폰 코드 결정
        if (credit < 1000) {
            return "AA";
        } else if (credit <= 2999) {
            return "BB";
        } else if (credit <= 3999) {
            if ("F".equalsIgnoreCase(gender) && addr.contains("송파구") && addr.contains("풍납1동")) {
                return "C2";
            } else {
                return "CC";
            }
        } else {
            return "DD";
        }
    }
}
