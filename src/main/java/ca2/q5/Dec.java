package ca2.q5;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Exact money arithmetic: amounts are BigDecimal while converting and are carried as long in 1/10000 INR (E4) afterwards. */
public final class Dec {
    private Dec() { }

    /** Throws NumberFormatException for anything that is not a plain decimal number. */
    public static BigDecimal parse(String s) {
        return new BigDecimal(s.trim());
    }

    /** BigDecimal -> E4 long, HALF_UP. */
    public static long e4(BigDecimal b) {
        return b.movePointRight(4).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    /** amount (in its own currency) * inr_rate -> INR E4, HALF_UP on the exact product. */
    public static long convert(BigDecimal amount, BigDecimal rate) {
        return e4(amount.multiply(rate));
    }

    /** "2026-03-01 10:15:00" / "2026-03-01T10:15:00Z" -> "2026-03-01"; null when the timestamp does not start with a date. */
    public static String date(String ts) {
        if (ts == null) return null;
        String t = ts.trim();
        if (t.length() < 10) return null;
        for (int i = 0; i < 10; i++) {
            char c = t.charAt(i);
            boolean dash = (i == 4 || i == 7);
            if (dash ? c != '-' : (c < '0' || c > '9')) return null;
        }
        return t.substring(0, 10);
    }
}
