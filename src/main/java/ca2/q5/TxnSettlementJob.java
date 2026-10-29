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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Stage A (first join): gateway_txns (ONE) with bank_settlements (MANY) on txn_id.
 * <pre>
 *   TxnMapper        : key (txn_id, tag 0), value "order_id \t amount \t ccy \t STATUS \t ts"
 *   SettlementMapper : key (txn_id, tag 1), value "settlement_id \t settled_E4 \t date"
 *   shuffle          : Partitioner on txn_id, sort (txn_id, tag), grouping on txn_id
 *   reducer          : the transaction arrives first and is the ONLY record held; settlements are streamed and only
 *                      accumulated (count, sum). A group that starts with a settlement has no transaction: ORPHAN_SETTLEMENT.
 * </pre>
 * Output: main = one enriched record per transaction
 *         "txn_id \t order_id \t amount \t ccy \t STATUS \t ts \t settled_sum_E4 \t settlement_count";
 *         MultipleOutputs ORPHAN_SETTLEMENT/part and summary_feed/part.
 */
public class TxnSettlementJob {

    static boolean header(String first, String name) {
        return first.trim().equalsIgnoreCase(name);
    }

    public static class TxnMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
        private final TaggedKey key = new TaggedKey();
        private final Text val = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            if (line.toString().trim().isEmpty()) return;
            String[] f = Csv.split(line.toString());
            if (header(f[0], "txn_id")) { ctx.getCounter(Q5Counter.TXNS_HEADER_SKIPPED).increment(1); return; }
            ctx.getCounter(Q5Counter.TXNS_READ).increment(1);
            if (f.length < 6 || f[0].trim().isEmpty() || f[1].trim().isEmpty()) { ctx.getCounter(Q5Counter.TXNS_BAD_FIELD_COUNT).increment(1); return; }
            try { Dec.parse(f[2]); } catch (NumberFormatException e) { ctx.getCounter(Q5Counter.TXNS_BAD_AMOUNT).increment(1); return; }
            String ccy = f[3].trim().toUpperCase();
            if (ccy.isEmpty()) { ctx.getCounter(Q5Counter.TXNS_BAD_CURRENCY).increment(1); return; }
            String status = f[4].trim().toUpperCase();
            if (!status.equals("SUCCESS") && !status.equals("FAILED") && !status.equals("REFUNDED")) { ctx.getCounter(Q5Counter.TXNS_BAD_STATUS).increment(1); return; }
            String ts = f[5].trim();
            if (Dec.date(ts) == null) ctx.getCounter(Q5Counter.TXNS_BAD_TS).increment(1);        // kept: only an orphan's conversion needs it
            key.set(f[0].trim(), 0);
            val.set(f[1].trim() + "\t" + f[2].trim() + "\t" + ccy + "\t" + status + "\t" + ts);
            ctx.write(key, val);
        }
    }

    public static class SettlementMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
        private final TaggedKey key = new TaggedKey();
        private final Text val = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            if (line.toString().trim().isEmpty()) return;
            String[] f = Csv.split(line.toString());
            if (header(f[0], "settlement_id")) { ctx.getCounter(Q5Counter.SETTLEMENTS_HEADER_SKIPPED).increment(1); return; }
            ctx.getCounter(Q5Counter.SETTLEMENTS_READ).increment(1);
            if (f.length < 4 || f[0].trim().isEmpty() || f[1].trim().isEmpty()) { ctx.getCounter(Q5Counter.SETTLEMENTS_BAD_FIELD_COUNT).increment(1); return; }
            long amount;
            try { amount = Dec.e4(Dec.parse(f[2])); } catch (NumberFormatException | ArithmeticException e) { ctx.getCounter(Q5Counter.SETTLEMENTS_BAD_AMOUNT).increment(1); return; }
            key.set(f[1].trim(), 1);
            val.set(f[0].trim() + "\t" + amount + "\t" + f[3].trim());
            ctx.write(key, val);
        }
    }

    public static class JoinReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        private final Text out = new Text();

        @Override
        protected void setup(Context ctx) { mos = new MultipleOutputs<>(ctx); }

        @Override
        protected void reduce(TaggedKey key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String txnId = key.getId();
            String txn = null;                      // the ONE record held in memory
            long settledSum = 0, settledCount = 0;  // running aggregate of the MANY side - nothing is buffered
            for (Text v : values) {
                if (key.getTag() == 0) {
                    String s = v.toString();
                    if (txn == null) txn = s;
                    else {
                        ctx.getCounter(Q5Counter.DUPLICATE_TXN_ID).increment(1);
                        if (s.compareTo(txn) < 0) txn = s;                   // conflicting copies: keep the smallest
                    }
                } else if (txn == null) {                                    // a settlement and no transaction
                    String[] f = v.toString().split("\t");                   // settlement_id, settled_E4, date
                    long e4 = Long.parseLong(f[1]);
                    out.set(f[0] + "\t" + txnId + "\t" + Money.format(e4) + "\t" + f[2]);
                    mos.write(NullWritable.get(), out, "ORPHAN_SETTLEMENT/part");
                    out.set("ORPHAN_SETTLEMENT\t0\t0\t" + e4);
                    mos.write(NullWritable.get(), out, "summary_feed/part");
                    ctx.getCounter(Q5Counter.ORPHAN_SETTLEMENT).increment(1);
                } else {
                    settledSum += Long.parseLong(v.toString().split("\t")[1]);
                    settledCount++;
                }
            }
            if (txn != null) {
                ctx.getCounter(Q5Counter.TXNS_JOINED).increment(1);
                if (settledCount == 0) ctx.getCounter(Q5Counter.TXNS_WITHOUT_SETTLEMENT).increment(1);
                if (settledCount > 1) ctx.getCounter(Q5Counter.TXNS_SETTLED_MORE_THAN_ONCE).increment(1);
                out.set(txnId + "\t" + txn + "\t" + settledSum + "\t" + settledCount);
                ctx.write(NullWritable.get(), out);
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }
    }

    public static Job build(Configuration conf, Path txns, Path settlements, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q5-1-txn-settlement-join");
        job.setJarByClass(TxnSettlementJob.class);
        MultipleInputs.addInputPath(job, txns, TextInputFormat.class, TxnMapper.class);
        MultipleInputs.addInputPath(job, settlements, TextInputFormat.class, SettlementMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(TaggedKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(IdPartitioner.class);
        job.setSortComparatorClass(TagSortComparator.class);
        job.setGroupingComparatorClass(IdGroupingComparator.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
