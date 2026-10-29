package ca2.q3;

import java.math.BigDecimal;

/** Money is carried as a long in units of 1/10000 (E4) so that sums are exact and order-independent. */
public final class Money {

    private Money() { }

    /** 129900 (E4) -> "12.99" ; always at least 2 decimals, never more than needed. */
    public static String format(long e4) {
        BigDecimal b = BigDecimal.valueOf(e4, 4).stripTrailingZeros();
        if (b.scale() < 2) b = b.setScale(2);
        return b.toPlainString();
    }
}
