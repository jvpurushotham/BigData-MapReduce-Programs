package ca2.q5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * fx_rates.csv (date, currency, inr_rate) from the Distributed Cache: the small side of a REPLICATED join, loaded in setup().
 * Lookup (currency, date): exact date; else the latest EARLIER date of that currency; else the earliest LATER date; else
 * the job fails (an order must never be converted with an invented rate). INR is 1 unless the file says otherwise.
 */
public final class FxTable {
    private final Map<String, TreeMap<String, BigDecimal>> byCcy = new HashMap<>();

    public static FxTable load(TaskInputOutputContext<?, ?, ?, ?> ctx) throws IOException {
        FxTable t = new FxTable();
        URI[] cached = ctx.getCacheFiles();
        if (cached == null || cached.length == 0) throw new IOException("fx_rates.csv was not added to the Distributed Cache");
        for (URI uri : cached) {
            Path p = new Path(uri);
            FileSystem fs = p.getFileSystem(ctx.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String[] f = Csv.split(line);
                    if (f[0].trim().equalsIgnoreCase("date")) continue;                         // header
                    String date = f.length >= 3 ? Dec.date(f[0]) : null;
                    BigDecimal rate = null;
                    if (date != null) {
                        try { rate = Dec.parse(f[2]); } catch (NumberFormatException e) { rate = null; }
                    }
                    String ccy = f.length >= 3 ? f[1].trim().toUpperCase() : "";
                    if (date == null || rate == null || rate.signum() <= 0 || ccy.isEmpty()) {
                        ctx.getCounter(Q5Counter.FX_ROWS_MALFORMED).increment(1);
                        continue;
                    }
                    TreeMap<String, BigDecimal> m = t.byCcy.get(ccy);
                    if (m == null) { m = new TreeMap<>(); t.byCcy.put(ccy, m); }
                    if (m.put(date, rate) != null) ctx.getCounter(Q5Counter.FX_DUPLICATE_ROWS).increment(1);   // the later row wins
                    ctx.getCounter(Q5Counter.FX_ROWS_LOADED).increment(1);
                }
            }
        }
        return t;
    }

    /** INR per 1 unit of {@code ccy} on {@code date} (date may be null/"" when unknown). */
    public BigDecimal rate(String ccy, String date, TaskInputOutputContext<?, ?, ?, ?> ctx) throws IOException {
        TreeMap<String, BigDecimal> m = byCcy.get(ccy);
        if (m == null) {
            if (ccy.equals("INR")) return BigDecimal.ONE;
            throw new IOException("no fx rate at all for currency " + ccy);
        }
        String d = date == null ? "" : date;
        BigDecimal r = m.get(d);
        if (r != null) { if (!ccy.equals("INR")) ctx.getCounter(Q5Counter.FX_CONVERSIONS).increment(1); return r; }
        Map.Entry<String, BigDecimal> e = m.floorEntry(d);
        if (e != null) { ctx.getCounter(Q5Counter.FX_FALLBACK_EARLIER_DATE).increment(1); return e.getValue(); }
        e = m.ceilingEntry(d);
        ctx.getCounter(Q5Counter.FX_FALLBACK_LATER_DATE).increment(1);
        return e.getValue();
    }
}
