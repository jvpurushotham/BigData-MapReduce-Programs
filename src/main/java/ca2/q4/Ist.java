package ca2.q4;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** IST (Asia/Kolkata) helpers. IST is a fixed +05:30 offset (no daylight saving), so plain arithmetic is exact. */
public final class Ist {
    public static final long OFFSET_MS = 19_800_000L;
    private static final long DAY_MS = 86_400_000L;
    private static final ZoneOffset ZONE = ZoneOffset.ofHoursMinutes(5, 30);
    private static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx");

    private Ist() { }

    /** Calendar day in IST as days since 1970-01-01. */
    public static long day(long epochMs) {
        return Math.floorDiv(epochMs + OFFSET_MS, DAY_MS);
    }

    /** yyyy-MM-dd of an IST day number. */
    public static String date(long istDay) {
        return LocalDate.ofEpochDay(istDay).toString();
    }

    /** ISO-8601 with millis and the +05:30 offset, e.g. 2026-10-01T10:15:30.123+05:30 */
    public static String iso(long epochMs) {
        return Instant.ofEpochMilli(epochMs).atOffset(ZONE).format(ISO);
    }

    /** Seconds with 3 decimals from milliseconds (exact): 90123 -> 90.123 */
    public static String seconds(long ms) {
        return (ms / 1000) + "." + String.format("%03d", ms % 1000);
    }
}
