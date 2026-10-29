package ca2.q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Single driver for Question 3.   Job 1 (reduce-side join) -> Job 2 (net revenue, replicated join) -> Job 3 (Top-N).
 *
 * Usage : hadoop jar ca2.jar ca2.q3.Q3Driver [-D key=value ...] <orders> <returns> <products.csv> <output_dir>
 *         (orders and returns may be a file or a directory)
 *
 * Optional -D switches
 *   q3.reducers=N            reducers of Job 3, routed by category (default 4; at least 3 are required)
 *   q3.reducers.12=N         reducers of Job 1 and Job 2 (default 2)
 *   q3.topn.n=N              the N of Top-N (default 5)
 *   q3.topn.bounded=false    experiment: mappers keep everything (more shuffle)
 *   q3.combiner=false        experiment: no Combiner in Job 2
 *   q3.keep.intermediate=true  keep <output>/net_lines and <output>/revenue (default false: they are deleted at the end;
 *                              run_q3.sh keeps them because the report needs them as evidence)
 *
 * Output layout : <output>/top5   <output>/orphan_returns   (+ net_lines, revenue when kept)
 * Exit status   : non-zero if any stage fails.
 */
public class Q3Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Q3Driver [-D key=value] <orders> <returns> <products.csv> <output_dir>");
            return 2;
        }
        Configuration conf = getConf();
        Path orders = new Path(args[0]);
        Path returns = new Path(args[1]);
        Path products = new Path(args[2]);
        Path out = new Path(args[3]);
        int r12 = conf.getInt("q3.reducers.12", 2);
        int r3 = conf.getInt("q3.reducers", 4);
        boolean keep = conf.getBoolean("q3.keep.intermediate", false);

        FileSystem fs = out.getFileSystem(conf);
        fs.delete(out, true);
        Path net = new Path(out, "net_lines");
        Path rev = new Path(out, "revenue");
        Path top = new Path(out, "top5");
        Path orphans = new Path(out, "orphan_returns");

        try {
            // ---- Job 1 : reduce-side join -------------------------------------------------------------
            Job j1 = JoinJob.build(conf, orders, returns, net, r12);
            if (!j1.waitForCompletion(true)) return fail("Job 1 (join) failed");
            printJoin(j1.getCounters());
            fs.mkdirs(orphans);                                          // orphan_returns-r-* are a result, not intermediate
            FileStatus[] o = fs.globStatus(new Path(net, "orphan_returns-r-*"));
            if (o != null) for (FileStatus st : o) fs.rename(st.getPath(), new Path(orphans, st.getPath().getName()));

            // ---- Job 2 : net revenue (replicated join) --------------------------------------------------
            Job j2 = RevenueJob.build(conf, new Path(net, "part-r-*"), products, rev, r12);
            if (!j2.waitForCompletion(true)) return fail("Job 2 (net revenue) failed");
            printRevenue(j2, conf.getBoolean("q3.combiner", true));

            // ---- Job 3 : Top-N per (category, month) ----------------------------------------------------
            Job j3 = TopNJob.build(conf, new Path(rev, "part-r-*"), products, top, r3);
            if (!j3.waitForCompletion(true)) return fail("Job 3 (Top-N) failed");
            printTop(j3, r3, fs, top);
            return 0;
        } finally {
            if (!keep) {
                fs.delete(net, true);                                    // delete intermediate directories
                fs.delete(rev, true);
            }
        }
    }

    private static int fail(String msg) {
        System.err.println("[Q3] " + msg);
        return 1;
    }

    private static void line(String name, long v) {
        System.out.println("[Q3]   " + name + " = " + v);
    }

    private static void printAll(Counters c, Q3Counter... ks) {
        for (Q3Counter k : ks) line(k.name(), c.findCounter(k).getValue());
    }

    private static void printJoin(Counters c) {
        System.out.println("[Q3] ---- Job 1 (reduce-side join) ----");
        printAll(c, Q3Counter.ORDERS_READ, Q3Counter.ORDERS_HEADER_SKIPPED, Q3Counter.ORDERS_BLANK_LINES,
                Q3Counter.ORDERS_BAD_FIELD_COUNT, Q3Counter.ORDERS_BAD_LINE_NO, Q3Counter.ORDERS_BAD_TIMESTAMP,
                Q3Counter.ORDERS_BAD_QTY, Q3Counter.ORDERS_BAD_PRICE, Q3Counter.ORDERS_BAD_PRODUCT,
                Q3Counter.PRICE_ROUNDED_TO_4_DECIMALS,
                Q3Counter.RETURNS_READ, Q3Counter.RETURNS_HEADER_SKIPPED, Q3Counter.RETURNS_BLANK_LINES,
                Q3Counter.RETURNS_BAD_FIELD_COUNT, Q3Counter.RETURNS_BAD_LINE_NO, Q3Counter.RETURNS_BAD_QTY,
                Q3Counter.RETURNS_BAD_TIMESTAMP,
                Q3Counter.ORDER_LINES_JOINED, Q3Counter.DUPLICATE_ORDER_LINE, Q3Counter.CONFLICTING_ORDER_LINE,
                Q3Counter.DUPLICATE_RETURN, Q3Counter.CANCELLED_LINES, Q3Counter.RETURNS_ON_CANCELLED_LINES,
                Q3Counter.LINES_WITH_MULTIPLE_RETURNS, Q3Counter.RETURN_EXCEEDS_ORDER, Q3Counter.FULLY_RETURNED_LINES,
                Q3Counter.ORPHAN_RETURNS, Q3Counter.NET_LINES_WRITTEN);
    }

    private static void printRevenue(Job j, boolean combiner) throws Exception {
        Counters c = j.getCounters();
        System.out.println("[Q3] ---- Job 2 (net revenue, combiner=" + combiner + ") ----");
        printAll(c, Q3Counter.PRODUCTS_LOADED, Q3Counter.DUPLICATE_PRODUCT_ROWS, Q3Counter.PRODUCT_ROWS_MALFORMED,
                Q3Counter.UNMAPPED_LINES, Q3Counter.GROUPS_WRITTEN);
        line("MAP_OUTPUT_RECORDS", c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
        line("MAP_OUTPUT_BYTES", c.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue());
        line("COMBINE_INPUT_RECORDS", c.findCounter(TaskCounter.COMBINE_INPUT_RECORDS).getValue());
        line("COMBINE_OUTPUT_RECORDS", c.findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS).getValue());
        line("REDUCE_SHUFFLE_BYTES", c.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue());
    }

    private static void printTop(Job j, int reducers, FileSystem fs, Path top) throws Exception {
        Counters c = j.getCounters();
        System.out.println("[Q3] ---- Job 3 (Top-" + j.getConfiguration().getInt("q3.topn.n", 5)
                + ", bounded=" + j.getConfiguration().getBoolean("q3.topn.bounded", true) + ", reducers=" + reducers + ") ----");
        printAll(c, Q3Counter.TOPN_INPUT_GROUP_ROWS, Q3Counter.TOPN_MAPPER_EVICTIONS, Q3Counter.TOPN_MAPPER_ENTRIES_EMITTED,
                Q3Counter.TOPN_GROUPS, Q3Counter.TOPN_ROWS_WRITTEN);
        line("MAP_OUTPUT_RECORDS", c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
        line("MAP_OUTPUT_BYTES", c.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue());
        line("REDUCE_SHUFFLE_BYTES", c.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue());

        CounterGroup g = c.getGroup("Q3_PARTITIONS");
        int used = 0;
        for (Counter k : g) {
            System.out.println("[Q3]   " + k.getName() + " ranked " + k.getValue() + " (category, month) groups");
            if (k.getValue() > 0) used++;
        }
        System.out.println("[Q3]   reducers that received data: " + used + " of " + reducers);
        if (used < 3 && j.getConfiguration().getInt("q3.topn.n", 5) > 0) {
            System.out.println("[Q3]   WARNING: fewer than 3 reducers used (the assignment requires at least 3): raise -Dq3.reducers");
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Q3Driver(), args));
    }
}
