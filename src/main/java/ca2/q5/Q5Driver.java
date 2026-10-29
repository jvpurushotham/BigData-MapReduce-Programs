package ca2.q5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Single driver for Question 5.   Stage A (txn x settlement on txn_id) -> Stage B (order x txn on order_id, classify) -> Stage C (summary).
 *
 * Usage : hadoop jar ca2.jar ca2.q5.Q5Driver [-D key=value ...] <orders> <gateway_txns> <bank_settlements> <fx_rates.csv> <output_dir>
 *         (orders, gateway_txns and bank_settlements may be a file or a directory)
 *
 * Optional -D switches
 *   q5.reducers=N              reducers of stage A and B (default 4)
 *   q5.refund.settlements=subtract|ignore   how settlements of REFUNDED transactions count towards "settled" (default subtract)
 *   q5.keep.intermediate=false delete stage_a / stage_b afterwards (default true: they are evidence)
 *
 * Output layout : <output>/UNPAID  UNDERPAID  OVERPAID  PAID_NOT_SETTLED  SETTLEMENT_MISMATCH  MATCHED  ORPHAN_TXN  ORPHAN_SETTLEMENT
 *                 <output>/summary/part-r-00000      (+ stage_a, stage_b when kept)
 * Exit status   : non-zero if any stage fails.
 */
public class Q5Driver extends Configured implements Tool {
    static final String[] CATEGORIES = { "UNPAID", "UNDERPAID", "OVERPAID", "PAID_NOT_SETTLED", "SETTLEMENT_MISMATCH", "MATCHED",
            "ORPHAN_TXN", "ORPHAN_SETTLEMENT" };

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: Q5Driver [-D key=value] <orders> <gateway_txns> <bank_settlements> <fx_rates.csv> <output_dir>");
            return 2;
        }
        Configuration conf = getConf();
        Path orders = new Path(args[0]), txns = new Path(args[1]), settlements = new Path(args[2]), fx = new Path(args[3]), out = new Path(args[4]);
        int reducers = conf.getInt("q5.reducers", 4);
        FileSystem fs = out.getFileSystem(conf);
        fs.delete(out, true);
        Path a = new Path(out, "stage_a"), b = new Path(out, "stage_b"), summary = new Path(out, "summary");

        // ---- Stage A : FIRST join, txn_id --------------------------------------------------------------
        Job ja = TxnSettlementJob.build(conf, txns, settlements, a, reducers);
        if (!ja.waitForCompletion(true)) return fail("Stage A (txn x settlement) failed");
        printA(ja);

        // ---- Stage B : SECOND join, order_id, classification ------------------------------------------
        Job jb = OrderJoinJob.build(conf, orders, new Path(a, "part-r-*"), fx, b, reducers);
        jb.getConfiguration().setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false);
        if (!jb.waitForCompletion(true)) return fail("Stage B (order x txn, classify) failed");
        printB(jb);

        // ---- Stage C : summary ------------------------------------------------------------------------
        Job jc = SummaryJob.build(conf, new Path[] { new Path(a, "summary_feed/part-r-*"), new Path(b, "summary_feed/part-r-*") }, summary);
        if (!jc.waitForCompletion(true)) return fail("Stage C (summary) failed");
        System.out.println("[Q5] ---- Stage C (summary) ----");
        System.out.println("[Q5]   SUMMARY_ROWS_WRITTEN = " + jc.getCounters().findCounter(Q5Counter.SUMMARY_ROWS_WRITTEN).getValue());

        // one directory per category at the top of the output (empty ones are created)
        for (String c : CATEGORIES) {
            Path from = new Path(c.equals("ORPHAN_SETTLEMENT") ? a : b, c);
            Path to = new Path(out, c);
            if (fs.exists(from)) fs.rename(from, to); else fs.mkdirs(to);
        }
        if (!conf.getBoolean("q5.keep.intermediate", true)) { fs.delete(a, true); fs.delete(b, true); }
        return 0;
    }

    private static int fail(String m) { System.err.println("[Q5] " + m); return 1; }

    private static void line(String n, long v) { System.out.println("[Q5]   " + n + " = " + v); }

    private static void print(Counters c, Q5Counter... ks) { for (Q5Counter k : ks) line(k.name(), c.findCounter(k).getValue()); }

    private static void shuffle(Counters c) {
        line("MAP_OUTPUT_RECORDS", c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
        line("MAP_OUTPUT_BYTES", c.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue());
        line("REDUCE_SHUFFLE_BYTES", c.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue());
        line("REDUCE_INPUT_GROUPS", c.findCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue());
        line("SPILLED_RECORDS", c.findCounter(TaskCounter.SPILLED_RECORDS).getValue());
    }

    private static void printA(Job j) throws Exception {
        Counters c = j.getCounters();
        System.out.println("[Q5] ---- Stage A (join 1: transactions x settlements on txn_id) ----");
        print(c, Q5Counter.TXNS_READ, Q5Counter.TXNS_HEADER_SKIPPED, Q5Counter.TXNS_BAD_FIELD_COUNT, Q5Counter.TXNS_BAD_AMOUNT,
                Q5Counter.TXNS_BAD_STATUS, Q5Counter.TXNS_BAD_CURRENCY, Q5Counter.TXNS_BAD_TS,
                Q5Counter.SETTLEMENTS_READ, Q5Counter.SETTLEMENTS_HEADER_SKIPPED, Q5Counter.SETTLEMENTS_BAD_FIELD_COUNT, Q5Counter.SETTLEMENTS_BAD_AMOUNT,
                Q5Counter.DUPLICATE_TXN_ID, Q5Counter.TXNS_JOINED, Q5Counter.TXNS_WITHOUT_SETTLEMENT, Q5Counter.TXNS_SETTLED_MORE_THAN_ONCE,
                Q5Counter.ORPHAN_SETTLEMENT);
        shuffle(c);
    }

    private static void printB(Job j) throws Exception {
        Counters c = j.getCounters();
        System.out.println("[Q5] ---- Stage B (join 2: orders x transactions on order_id; classification) ----");
        print(c, Q5Counter.ORDERS_READ, Q5Counter.ORDERS_HEADER_SKIPPED, Q5Counter.ORDERS_BAD_FIELD_COUNT, Q5Counter.ORDERS_BAD_AMOUNT,
                Q5Counter.ORDERS_BAD_CURRENCY, Q5Counter.ORDERS_BAD_TS, Q5Counter.FX_ROWS_LOADED, Q5Counter.FX_ROWS_MALFORMED, Q5Counter.FX_DUPLICATE_ROWS,
                Q5Counter.DUPLICATE_ORDER_ID, Q5Counter.ORDERS_JOINED, Q5Counter.ORDERS_WITHOUT_TXN, Q5Counter.ORDERS_FAILED_AND_SUCCESS,
                Q5Counter.ORDERS_WITH_REFUND, Q5Counter.ORDERS_NON_INR, Q5Counter.TXN_CURRENCY_DIFFERS_FROM_ORDER, Q5Counter.NEGATIVE_NET_PAID,
                Q5Counter.SETTLEMENTS_ON_FAILED_TXN, Q5Counter.SETTLEMENTS_ON_REFUND_TXN, Q5Counter.FX_CONVERSIONS,
                Q5Counter.FX_FALLBACK_EARLIER_DATE, Q5Counter.FX_FALLBACK_LATER_DATE,
                Q5Counter.CAT_UNPAID, Q5Counter.CAT_UNDERPAID, Q5Counter.CAT_OVERPAID, Q5Counter.CAT_PAID_NOT_SETTLED,
                Q5Counter.CAT_SETTLEMENT_MISMATCH, Q5Counter.CAT_MATCHED, Q5Counter.ORPHAN_TXN);
        shuffle(c);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Q5Driver(), args));
    }
}
