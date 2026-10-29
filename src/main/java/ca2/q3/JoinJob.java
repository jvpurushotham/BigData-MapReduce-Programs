package ca2.q3;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Job 1 (question 3a): REDUCE-SIDE JOIN of orders and returns on (order_id, line_no).
 *
 * Two mappers (MultipleInputs), one per input, both emit  key = "order_id|line_no" and a tagged value:
 *     orders  -> "O \t month \t product_id \t qty \t unit_price_E4 \t status"
 *     returns -> "R \t return_id \t qty_returned \t original csv row"
 * The reducer sees the order line and all of its returns together and
 *   * drops CANCELLED lines,
 *   * net_qty = qty - sum(qty_returned), capped so that it never goes below 0 (RETURN_EXCEEDS_ORDER),
 *   * writes the returns that match no order line to the MultipleOutputs file  orphan_returns,
 *   * emits one clean line per surviving order line:
 *     order_id, line_no, month, product_id, qty, returned, net_qty, unit_price_E4        (TAB separated)
 *
 * The month is the month of the ORDER timestamp, never of the return timestamp.
 *
 * Rules for dirty data (all counted): malformed rows are rejected on the mapper side; identical copies of an order line
 * collapse to one (DUPLICATE_ORDER_LINE); differing copies keep the lexicographically smallest record
 * (CONFLICTING_ORDER_LINE); the same return_id repeated for a line counts once (DUPLICATE_RETURN), while two DIFFERENT
 * return_ids for the same line are both subtracted. A return that points at a CANCELLED line is not an orphan (the line
 * exists); it is dropped with the line (RETURNS_ON_CANCELLED_LINES).
 *
 * A Combiner is not applicable: the reducer needs the order record and the returns of the same key together.
 */
public class JoinJob {

    static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withResolverStyle(ResolverStyle.STRICT);

    // ------------------------------------------------------------------------------------------ orders mapper
    public static class OrdersMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            if (s.trim().isEmpty()) { ctx.getCounter(Q3Counter.ORDERS_BLANK_LINES).increment(1); return; }
            String[] f = Csv.split(s);
            if (f[0].trim().equalsIgnoreCase("order_id")) { ctx.getCounter(Q3Counter.ORDERS_HEADER_SKIPPED).increment(1); return; }
            ctx.getCounter(Q3Counter.ORDERS_READ).increment(1);
            if (f.length != 8) { ctx.getCounter(Q3Counter.ORDERS_BAD_FIELD_COUNT).increment(1); return; }

            String orderId = f[0].trim();
            int lineNo;
            try { lineNo = Integer.parseInt(f[1].trim()); } catch (NumberFormatException e) {
                ctx.getCounter(Q3Counter.ORDERS_BAD_LINE_NO).increment(1); return;
            }
            if (orderId.isEmpty()) { ctx.getCounter(Q3Counter.ORDERS_BAD_LINE_NO).increment(1); return; }
            String ts = f[2].trim();
            try { TS.parse(ts); } catch (Exception e) { ctx.getCounter(Q3Counter.ORDERS_BAD_TIMESTAMP).increment(1); return; }
            String product = f[4].trim();
            if (product.isEmpty()) { ctx.getCounter(Q3Counter.ORDERS_BAD_PRODUCT).increment(1); return; }
            long qty;
            try { qty = Long.parseLong(f[5].trim()); } catch (NumberFormatException e) {
                ctx.getCounter(Q3Counter.ORDERS_BAD_QTY).increment(1); return;
            }
            if (qty <= 0) { ctx.getCounter(Q3Counter.ORDERS_BAD_QTY).increment(1); return; }
            long priceE4;
            try {
                BigDecimal p = new BigDecimal(f[6].trim());
                if (p.signum() < 0) throw new NumberFormatException("negative price");
                if (p.scale() > 4) ctx.getCounter(Q3Counter.PRICE_ROUNDED_TO_4_DECIMALS).increment(1);
                priceE4 = p.movePointRight(4).setScale(0, RoundingMode.HALF_UP).longValueExact();
            } catch (NumberFormatException | ArithmeticException e) {
                ctx.getCounter(Q3Counter.ORDERS_BAD_PRICE).increment(1); return;
            }
            String status = f[7].trim();

            outKey.set(orderId + "|" + lineNo);
            outVal.set("O\t" + ts.substring(0, 7) + "\t" + product + "\t" + qty + "\t" + priceE4 + "\t" + status);
            ctx.write(outKey, outVal);
        }
    }

    // ----------------------------------------------------------------------------------------- returns mapper
    public static class ReturnsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            if (s.trim().isEmpty()) { ctx.getCounter(Q3Counter.RETURNS_BLANK_LINES).increment(1); return; }
            String[] f = Csv.split(s);
            if (f[0].trim().equalsIgnoreCase("return_id")) { ctx.getCounter(Q3Counter.RETURNS_HEADER_SKIPPED).increment(1); return; }
            ctx.getCounter(Q3Counter.RETURNS_READ).increment(1);
            if (f.length != 5 || f[0].trim().isEmpty() || f[1].trim().isEmpty()) {
                ctx.getCounter(Q3Counter.RETURNS_BAD_FIELD_COUNT).increment(1); return;
            }
            int lineNo;
            try { lineNo = Integer.parseInt(f[2].trim()); } catch (NumberFormatException e) {
                ctx.getCounter(Q3Counter.RETURNS_BAD_LINE_NO).increment(1); return;
            }
            long qty;
            try { qty = Long.parseLong(f[3].trim()); } catch (NumberFormatException e) {
                ctx.getCounter(Q3Counter.RETURNS_BAD_QTY).increment(1); return;
            }
            if (qty <= 0) { ctx.getCounter(Q3Counter.RETURNS_BAD_QTY).increment(1); return; }
            try { TS.parse(f[4].trim()); } catch (Exception e) { ctx.getCounter(Q3Counter.RETURNS_BAD_TIMESTAMP).increment(1); return; }

            outKey.set(f[1].trim() + "|" + lineNo);
            outVal.set("R\t" + f[0].trim() + "\t" + qty + "\t" + s.trim());
            ctx.write(outKey, outVal);
        }
    }

    // ---------------------------------------------------------------------------------------------- reducer
    public static class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        private final Text out = new Text();

        @Override
        protected void setup(Context ctx) {
            mos = new MultipleOutputs<>(ctx);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            TreeSet<String> orders = new TreeSet<>();
            long orderCopies = 0;
            TreeMap<String, String> returns = new TreeMap<>();         // return_id -> "qty \t raw row"
            for (Text v : values) {
                String s = v.toString();
                if (s.charAt(0) == 'O') {
                    orders.add(s.substring(2));
                    orderCopies++;
                } else {
                    String[] p = s.split("\t", 4);                     // R, return_id, qty, raw
                    String rest = p[2] + "\t" + p[3];
                    String prev = returns.get(p[1]);
                    if (prev != null) {
                        ctx.getCounter(Q3Counter.DUPLICATE_RETURN).increment(1);
                        if (rest.compareTo(prev) < 0) returns.put(p[1], rest);
                    } else {
                        returns.put(p[1], rest);
                    }
                }
            }

            if (orders.isEmpty()) {                                    // returns that match no order line
                for (String r : returns.values()) {
                    ctx.getCounter(Q3Counter.ORPHAN_RETURNS).increment(1);
                    out.set(r.substring(r.indexOf('\t') + 1));
                    mos.write(NullWritable.get(), out, "orphan_returns");
                }
                return;
            }

            ctx.getCounter(Q3Counter.ORDER_LINES_JOINED).increment(1);
            if (orderCopies > 1) {
                ctx.getCounter(orders.size() == 1 ? Q3Counter.DUPLICATE_ORDER_LINE : Q3Counter.CONFLICTING_ORDER_LINE)
                        .increment(orderCopies - 1);
            }
            String[] o = orders.first().split("\t", -1);               // month, product, qty, priceE4, status
            if (o[4].equalsIgnoreCase("CANCELLED")) {
                ctx.getCounter(Q3Counter.CANCELLED_LINES).increment(1);
                if (!returns.isEmpty()) ctx.getCounter(Q3Counter.RETURNS_ON_CANCELLED_LINES).increment(returns.size());
                return;
            }

            long qty = Long.parseLong(o[2]);
            long returned = 0;
            for (String r : returns.values()) returned += Long.parseLong(r.substring(0, r.indexOf('\t')));
            if (returns.size() > 1) ctx.getCounter(Q3Counter.LINES_WITH_MULTIPLE_RETURNS).increment(1);
            long net = qty - returned;
            if (net < 0) {                                             // returns exceed the ordered quantity: cap
                ctx.getCounter(Q3Counter.RETURN_EXCEEDS_ORDER).increment(1);
                net = 0;
            }
            if (net == 0) ctx.getCounter(Q3Counter.FULLY_RETURNED_LINES).increment(1);

            String k = key.toString();
            int bar = k.lastIndexOf('|');
            out.set(k.substring(0, bar) + "\t" + k.substring(bar + 1) + "\t" + o[0] + "\t" + o[1] + "\t" + qty + "\t"
                    + returned + "\t" + net + "\t" + o[3]);
            ctx.getCounter(Q3Counter.NET_LINES_WRITTEN).increment(1);
            ctx.write(NullWritable.get(), out);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static Job build(Configuration conf, Path orders, Path returns, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q3-1-reduce-side-join");
        job.setJarByClass(JoinJob.class);
        MultipleInputs.addInputPath(job, orders, TextInputFormat.class, OrdersMapper.class);
        MultipleInputs.addInputPath(job, returns, TextInputFormat.class, ReturnsMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(reducers);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
