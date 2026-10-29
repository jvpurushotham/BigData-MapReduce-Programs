package ca2.q4;

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
 * Single driver for Question 4.   Job 1 (sessions, secondary sort) -> Job 2 (funnel per IST date).
 *
 * Usage : hadoop jar ca2.jar ca2.q4.Q4Driver [-D key=value ...] <clicks dir or glob> <output_dir>
 *
 * Optional -D switches
 *   q4.reducers=N          reducers of Job 1 (partitioned by user_id; default 4)
 *   q4.reducers.2=N        reducers of Job 2 (default 1: one sorted report file)
 *   q4.gap.minutes=N       session gap (default 30, "strictly greater than")
 *   q4.bot.events=N        bot rule 1: more than N events (default 500)
 *   q4.bot.avggap.ms=N     bot rule 2: average inter-event gap below N ms (default 1000)
 *   q4.keep.intermediate=false  delete <output>/session_job/funnel_input afterwards (default true: it is evidence)
 * Memory proof (question 4): add  -D mapreduce.reduce.memory.mb=512 -D mapreduce.reduce.java.opts=-Xmx410m
 *
 * Output layout : <output>/session_job/{sessions,bots,funnel_input}/part-r-*   <output>/funnel/part-r-00000
 * Exit status   : non-zero if any stage fails.
 */
public class Q4Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Q4Driver [-D key=value] <clicks dir or glob> <output_dir>");
            return 2;
        }
        Configuration conf = getConf();
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileSystem fs = out.getFileSystem(conf);
        fs.delete(out, true);
        Path j1out = new Path(out, "session_job");
        Path funnel = new Path(out, "funnel");

        Job j1 = SessionJob.build(conf, in, j1out, conf.getInt("q4.reducers", 4));
        if (!j1.waitForCompletion(true)) return fail("Job 1 (sessions) failed");
        printSessions(j1);

        Job j2 = FunnelJob.build(conf, new Path(j1out, "funnel_input/part-r-*"), funnel, conf.getInt("q4.reducers.2", 1));
        if (!j2.waitForCompletion(true)) return fail("Job 2 (funnel) failed");
        printFunnel(j2);

        if (!conf.getBoolean("q4.keep.intermediate", true)) fs.delete(new Path(j1out, "funnel_input"), true);
        return 0;
    }

    private static int fail(String msg) {
        System.err.println("[Q4] " + msg);
        return 1;
    }

    private static void line(String name, long v) {
        System.out.println("[Q4]   " + name + " = " + v);
    }

    private static void printSessions(Job j) throws Exception {
        Counters c = j.getCounters();
        Configuration cf = j.getConfiguration();
        System.out.println("[Q4] ---- Job 1 (sessions, secondary sort; reducers=" + j.getNumReduceTasks()
                + ", gap=" + cf.getLong("q4.gap.minutes", 30) + " min, bot > " + cf.getLong("q4.bot.events", 500)
                + " events or avg gap < " + cf.getLong("q4.bot.avggap.ms", 1000) + " ms) ----");
        for (Q4Counter k : Q4Counter.values()) {
            if (k == Q4Counter.FUNNEL_ROWS_WRITTEN) continue;
            line(k.name(), c.findCounter(k).getValue());
        }
        line("MAP_INPUT_RECORDS", c.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
        line("MAP_OUTPUT_RECORDS", c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
        line("MAP_OUTPUT_BYTES", c.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue());
        line("REDUCE_SHUFFLE_BYTES", c.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue());
        line("REDUCE_INPUT_GROUPS(users)", c.findCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue());
        line("REDUCE_INPUT_RECORDS", c.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue());
        line("SPILLED_RECORDS", c.findCounter(TaskCounter.SPILLED_RECORDS).getValue());
        line("GC_TIME_MILLIS", c.findCounter(TaskCounter.GC_TIME_MILLIS).getValue());
        line("COMMITTED_HEAP_BYTES(sum of tasks)", c.findCounter(TaskCounter.COMMITTED_HEAP_BYTES).getValue());
        line("PHYSICAL_MEMORY_BYTES(sum of tasks)", c.findCounter(TaskCounter.PHYSICAL_MEMORY_BYTES).getValue());
        System.out.println("[Q4]   reduce memory.mb = " + cf.get("mapreduce.reduce.memory.mb") + ", reduce java.opts = " + cf.get("mapreduce.reduce.java.opts"));
    }

    private static void printFunnel(Job j) throws Exception {
        Counters c = j.getCounters();
        System.out.println("[Q4] ---- Job 2 (funnel per IST date) ----");
        line("FUNNEL_ROWS_WRITTEN", c.findCounter(Q4Counter.FUNNEL_ROWS_WRITTEN).getValue());
        line("MAP_OUTPUT_RECORDS", c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
        line("COMBINE_INPUT_RECORDS", c.findCounter(TaskCounter.COMBINE_INPUT_RECORDS).getValue());
        line("COMBINE_OUTPUT_RECORDS", c.findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS).getValue());
        line("REDUCE_SHUFFLE_BYTES", c.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue());
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Q4Driver(), args));
    }
}
