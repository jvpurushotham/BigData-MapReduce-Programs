package ca2.q5;

import java.io.IOException;
import java.math.BigDecimal;

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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Stage B (second join + classification): orders (ONE) with the enriched transactions of stage A (MANY) on order_id.
 * <pre>
 *   OrderMapper : key (order_id, tag 0), value "customer \t amount \t ccy \t ts"
 *   TxnMapper   : key (order_id, tag 1), value "txn_id \t amount \t ccy \t STATUS \t ts \t settled_sum_E4 \t settlement_count"
 *   reducer     : the order arrives first (only record held); transactions are streamed into running sums
 *                 (success, refunded, settled, counters) - NEVER buffered. fx_rates.csv is a replicated join (Distributed Cache).
 * </pre>
 * Classification (first rule that fits), all values in INR with tolerance Rs 1.00 (10000 in E4):
 * <ol><li>UNPAID: net paid = 0   <li>UNDERPAID / OVERPAID: |net - order| &gt; 1.00
 *     <li>PAID_NOT_SETTLED: a SUCCESS txn without settlement   <li>SETTLEMENT_MISMATCH: |settled - net| &gt; 1.00   <li>MATCHED</ol>
 * A group that starts with a transaction has no order: ORPHAN_TXN.
 */
public class OrderJoinJob {
    static final long TOLERANCE_E4 = 10_000L;

    public static class OrderMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
        private final TaggedKey key = new TaggedKey();
        private final Text val = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            if (line.toString().trim().isEmpty()) return;
            String[] f = Csv.split(line.toString());
            if (TxnSettlementJob.header(f[0], "order_id")) { ctx.getCounter(Q5Counter.ORDERS_HEADER_SKIPPED).increment(1); return; }
            ctx.getCounter(Q5Counter.ORDERS_READ).increment(1);
            if (f.length < 5 || f[0].trim().isEmpty()) { ctx.getCounter(Q5Counter.ORDERS_BAD_FIELD_COUNT).increment(1); return; }
            try { Dec.parse(f[2]); } catch (NumberFormatException e) { ctx.getCounter(Q5Counter.ORDERS_BAD_AMOUNT).increment(1); return; }
            String ccy = f[3].trim().toUpperCase();
            if (ccy.isEmpty()) { ctx.getCounter(Q5Counter.ORDERS_BAD_CURRENCY).increment(1); return; }
            if (Dec.date(f[4]) == null) { ctx.getCounter(Q5Counter.ORDERS_BAD_TS).increment(1); return; }
            key.set(f[0].trim(), 0);
            val.set(f[1].trim() + "\t" + f[2].trim() + "\t" + ccy + "\t" + f[4].trim());
            ctx.write(key, val);
        }
    }

    public static class TxnMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
        private final TaggedKey key = new TaggedKey();
        private final Text val = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            int tab = s.indexOf('\t');                       // txn_id \t order_id \t rest...
            int tab2 = s.indexOf('\t', tab + 1);
            key.set(s.substring(tab + 1, tab2), 1);
            val.set(s.substring(0, tab) + s.substring(tab2));   // txn_id + \t amount ...
            ctx.write(key, val);
        }
    }

    public static class ClassifyReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        private FxTable fx;
        private boolean subtractRefundSettlements;
        private final Text out = new Text();

        // ---- the order (ONE) ----
        private String orderId, customer, orderCcy, orderAmount, orderDate;
        private long orderInr;
        // ---- running aggregates over the MANY side ----
        private long success, refunded, settled, txns, nSuccess, nRefund, nFailed;
        private boolean unsettledSuccess;

        @Override
        protected void setup(Context ctx) throws IOException {
            mos = new MultipleOutputs<>(ctx);
            fx = FxTable.load(ctx);
            subtractRefundSettlements = !"ignore".equalsIgnoreCase(ctx.getConfiguration().get("q5.refund.settlements", "subtract"));
        }

        @Override
        protected void reduce(TaggedKey key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            orderId = key.getId();
            String order = null;                          // the ONE record held in memory
            boolean prepared = false;
            success = refunded = settled = txns = nSuccess = nRefund = nFailed = 0;
            unsettledSuccess = false;
            for (Text v : values) {
                if (key.getTag() == 0) {
                    String s = v.toString();
                    if (order == null) order = s;
                    else {
                        ctx.getCounter(Q5Counter.DUPLICATE_ORDER_ID).increment(1);
                        if (s.compareTo(order) < 0) order = s;
                    }
                } else if (order == null) {
                    orphanTxn(v.toString(), ctx);
                } else {
                    if (!prepared) { prepare(order, ctx); prepared = true; }
                    accumulate(v.toString(), ctx);
                }
            }
            if (order != null) {
                if (!prepared) prepare(order, ctx);
                classify(ctx);
            }
        }

        private void prepare(String order, Context ctx) throws IOException {
            String[] f = order.split("\t");               // customer, amount, ccy, ts
            customer = f[0]; orderAmount = f[1]; orderCcy = f[2]; orderDate = Dec.date(f[3]);
            orderInr = Dec.convert(Dec.parse(f[1]), fx.rate(orderCcy, orderDate, ctx));
            if (!orderCcy.equals("INR")) ctx.getCounter(Q5Counter.ORDERS_NON_INR).increment(1);
        }

        private void accumulate(String txn, Context ctx) throws IOException {
            String[] f = txn.split("\t");                 // txn_id, amount, ccy, STATUS, ts, settled_E4, count
            String ccy = f[2], status = f[3];
            long inr = Dec.convert(Dec.parse(f[1]), fx.rate(ccy, orderDate, ctx));   // rate of the ORDER date
            long rawSettled = Long.parseLong(f[5]);
            long count = Long.parseLong(f[6]);
            txns++;
            if (!ccy.equals(orderCcy)) ctx.getCounter(Q5Counter.TXN_CURRENCY_DIFFERS_FROM_ORDER).increment(1);
            switch (status) {
                case "SUCCESS":
                    success += inr; nSuccess++; settled += rawSettled;
                    if (count == 0) unsettledSuccess = true;
                    break;
                case "REFUNDED":
                    refunded += Math.abs(inr); nRefund++;
                    if (count > 0) {
                        ctx.getCounter(Q5Counter.SETTLEMENTS_ON_REFUND_TXN).increment(1);
                        if (subtractRefundSettlements) settled -= Math.abs(rawSettled);
                    }
                    break;
                default:                                   // FAILED
                    nFailed++;
                    if (count > 0) ctx.getCounter(Q5Counter.SETTLEMENTS_ON_FAILED_TXN).increment(1);
                    break;
            }
        }

        private void classify(Context ctx) throws IOException, InterruptedException {
            long net = success - refunded;
            Q5Counter cat;
            String name;
            if (txns == 0) ctx.getCounter(Q5Counter.ORDERS_WITHOUT_TXN).increment(1);
            if (nFailed > 0 && nSuccess > 0) ctx.getCounter(Q5Counter.ORDERS_FAILED_AND_SUCCESS).increment(1);
            if (nRefund > 0) ctx.getCounter(Q5Counter.ORDERS_WITH_REFUND).increment(1);
            if (net < 0) ctx.getCounter(Q5Counter.NEGATIVE_NET_PAID).increment(1);
            ctx.getCounter(Q5Counter.ORDERS_JOINED).increment(1);
            if (net == 0) { name = "UNPAID"; cat = Q5Counter.CAT_UNPAID; }
            else if (Math.abs(net - orderInr) > TOLERANCE_E4) {
                if (net < orderInr) { name = "UNDERPAID"; cat = Q5Counter.CAT_UNDERPAID; }
                else { name = "OVERPAID"; cat = Q5Counter.CAT_OVERPAID; }
            }
            else if (unsettledSuccess) { name = "PAID_NOT_SETTLED"; cat = Q5Counter.CAT_PAID_NOT_SETTLED; }
            else if (Math.abs(settled - net) > TOLERANCE_E4) { name = "SETTLEMENT_MISMATCH"; cat = Q5Counter.CAT_SETTLEMENT_MISMATCH; }
            else { name = "MATCHED"; cat = Q5Counter.CAT_MATCHED; }
            ctx.getCounter(cat).increment(1);
            out.set(orderId + "\t" + customer + "\t" + orderCcy + "\t" + orderAmount + "\t" + Money.format(orderInr) + "\t" + Money.format(net)
                    + "\t" + Money.format(settled) + "\t" + txns + "\t" + nSuccess + "\t" + nRefund + "\t" + nFailed + "\t" + Money.format(net - orderInr));
            mos.write(NullWritable.get(), out, name + "/part");
            out.set(name + "\t" + orderInr + "\t" + net + "\t" + settled);
            mos.write(NullWritable.get(), out, "summary_feed/part");
        }

        /** Transaction whose order does not exist: converted at the rate of its own date. */
        private void orphanTxn(String txn, Context ctx) throws IOException, InterruptedException {
            String[] f = txn.split("\t");                 // txn_id, amount, ccy, STATUS, ts, settled_E4, count
            long inr = Dec.convert(Dec.parse(f[1]), fx.rate(f[2], Dec.date(f[4]), ctx));
            long raw = Long.parseLong(f[5]);
            long signedInr, signedSettled;
            switch (f[3]) {
                case "SUCCESS": signedInr = inr; signedSettled = raw; break;
                case "REFUNDED": signedInr = -Math.abs(inr); signedSettled = subtractRefundSettlements ? -Math.abs(raw) : 0; break;
                default: signedInr = 0; signedSettled = 0; break;
            }
            out.set(f[0] + "\t" + orderId + "\t" + f[1] + "\t" + f[2] + "\t" + f[3] + "\t" + f[4] + "\t" + Money.format(signedInr)
                    + "\t" + Money.format(signedSettled) + "\t" + f[6]);
            mos.write(NullWritable.get(), out, "ORPHAN_TXN/part");
            out.set("ORPHAN_TXN\t0\t" + signedInr + "\t" + signedSettled);
            mos.write(NullWritable.get(), out, "summary_feed/part");
            ctx.getCounter(Q5Counter.ORPHAN_TXN).increment(1);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }
    }

    public static Job build(Configuration conf, Path orders, Path enrichedTxns, Path fxRates, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q5-2-order-join-classify");
        job.setJarByClass(OrderJoinJob.class);
        job.addCacheFile(fxRates.toUri());
        MultipleInputs.addInputPath(job, orders, TextInputFormat.class, OrderMapper.class);
        MultipleInputs.addInputPath(job, enrichedTxns, TextInputFormat.class, TxnMapper.class);
        job.setReducerClass(ClassifyReducer.class);
        job.setMapOutputKeyClass(TaggedKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(IdPartitioner.class);
        job.setSortComparatorClass(TagSortComparator.class);
        job.setGroupingComparatorClass(IdGroupingComparator.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setNumReduceTasks(reducers);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
