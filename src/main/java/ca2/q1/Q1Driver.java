package ca2.q1;

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
 * Single driver for Question 1. Chains  A (clean+dedup) -> B (stats) -> C (outliers, replicated join).
 *
 * Usage : hadoop jar ca2.jar ca2.q1.Q1Driver [-D key=value ...] <input_dir> <output_dir>
 *
 * Optional -D switches
 *   q1.combiner=true|false   use the correct Combiner in Job B            (default true)
 *   q1.wrong=true            use the deliberately wrong averaging Combiner and stop after Job B (1d)
 *   q1.reducers=N            reducers for every job                        (default 2)
 *   q1.keep.clean=true       keep the intermediate clean directory         (default false)
 *   mapreduce.input.fileinputformat.split.maxsize=BYTES   force more input splits (used for 1d)
 *
 * Output layout : <output>/stats (or stats_wrong)  and  <output>/outliers
 * Exit status   : non-zero if any stage fails.
 */
public class Q1Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Q1Driver [-D key=value] <input_dir> <output_dir>");
            return 2;
        }
        Configuration conf = getConf();
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        boolean combiner = conf.getBoolean("q1.combiner", true);
        boolean wrong = conf.getBoolean("q1.wrong", false);
        boolean keepClean = conf.getBoolean("q1.keep.clean", false);
        int reducers = conf.getInt("q1.reducers", 2);

        FileSystem fs = out.getFileSystem(conf);
        fs.delete(out, true);
        Path clean = new Path(out, "clean_tmp");
        Path stats = new Path(out, wrong ? "stats_wrong" : "stats");
        Path outliers = new Path(out, "outliers");

        try {
            // ---- Job A : clean + de-duplicate ------------------------------------------------------
            Job a = CleanJob.build(conf, in, clean, reducers);
            if (!a.waitForCompletion(true)) return fail("Job A (clean/dedup) failed");
            printCleanCounters(a);

            // ---- Job B : single-pass statistics ------------------------------------------------------
            Job b = StatsJob.build(conf, clean, stats, reducers, combiner, wrong);
            if (!b.waitForCompletion(true)) return fail("Job B (stats) failed");
            printStatsCounters(b, wrong ? "WRONG averaging combiner" : combiner ? "with combiner" : "WITHOUT combiner");

            if (wrong) {
                System.out.println("[Q1] wrong variant finished; skipping Job C");
                return 0;
            }

            // ---- Job C : outliers via Distributed Cache (replicated join) --------------------------
            Job c = OutlierJob.build(conf, clean, stats, outliers, reducers);
            if (!c.waitForCompletion(true)) return fail("Job C (outliers) failed");
            System.out.println("[Q1] OUTLIER_READINGS = " + c.getCounters().findCounter(Q1Counter.OUTLIER_READINGS).getValue()
                    + ", MONTHS_WITHOUT_STATS = " + c.getCounters().findCounter(Q1Counter.MONTHS_WITHOUT_STATS).getValue());
            return 0;
        } finally {
            if (!keepClean) fs.delete(clean, true);      // delete intermediate directory
        }
    }

    private static int fail(String msg) {
        System.err.println("[Q1] " + msg);
        return 1;
    }

    private static void printCleanCounters(Job a) throws Exception {
        Counters c = a.getCounters();
        System.out.println("[Q1] ---- Job A counters ----");
        for (Q1Counter k : new Q1Counter[] {
                Q1Counter.RECORDS_READ, Q1Counter.HEADER_SKIPPED, Q1Counter.REJECTED_FIELD_COUNT,
                Q1Counter.REJECTED_EMPTY_FIELD, Q1Counter.REJECTED_BAD_TIMESTAMP, Q1Counter.REJECTED_BAD_KWH,
                Q1Counter.REJECTED_NEGATIVE_KWH, Q1Counter.DUPLICATE_READING, Q1Counter.UNIQUE_READINGS }) {
            System.out.println("[Q1]   " + k + " = " + c.findCounter(k).getValue());
        }
    }

    private static void printStatsCounters(Job b, String label) throws Exception {
        Counters c = b.getCounters();
        System.out.println("[Q1] ---- Job B (" + label + ") ----");
        System.out.println("[Q1]   MAP_INPUT_RECORDS       = " + c.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
        System.out.println("[Q1]   MAP_OUTPUT_RECORDS      = " + c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
        System.out.println("[Q1]   MAP_OUTPUT_BYTES        = " + c.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue());
        System.out.println("[Q1]   COMBINE_INPUT_RECORDS   = " + c.findCounter(TaskCounter.COMBINE_INPUT_RECORDS).getValue());
        System.out.println("[Q1]   COMBINE_OUTPUT_RECORDS  = " + c.findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS).getValue());
        System.out.println("[Q1]   REDUCE_SHUFFLE_BYTES    = " + c.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue());
        System.out.println("[Q1]   REDUCE_INPUT_RECORDS    = " + c.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue());
        System.out.println("[Q1]   NEGATIVE_VARIANCE_CLAMPED = " + c.findCounter(Q1Counter.NEGATIVE_VARIANCE_CLAMPED).getValue());
        System.out.println("[Q1]   SPLITS (launched maps)  = " + c.findCounter(TaskCounter.MAP_INPUT_RECORDS).getDisplayName()
                + " / TOTAL_LAUNCHED_MAPS = "
                + c.findCounter(org.apache.hadoop.mapreduce.JobCounter.TOTAL_LAUNCHED_MAPS).getValue());
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Q1Driver(), args));
    }
}
