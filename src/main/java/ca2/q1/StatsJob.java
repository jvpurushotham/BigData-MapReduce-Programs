package ca2.q1;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job B (questions 1b, 1c, 1d): single-pass statistics per (city, meter_id, month).
 *
 * Map     : ("city,meter,month", microKwh)  ->  (key, StatsWritable(1, v, v^2, v, v))
 * Combine : merges StatsWritables (correct Combiner)      OR  WrongAvgCombiner (question 1d)
 * Reduce  : merges, then derives mean and population standard deviation
 *             mean = sum / n          variance = sumSq / n - mean^2   (clamped at 0)
 *
 * Output  : "city,meter,month" TAB "count,min,max,mean,stddev"   (kWh, fixed decimals so diff is meaningful)
 */
public class StatsJob {

    public static class StatsMapper extends Mapper<LongWritable, Text, Text, StatsWritable> {
        private final Text outKey = new Text();
        private final StatsWritable outVal = new StatsWritable();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            int tab = s.indexOf('\t');
            outKey.set(s.substring(0, tab));
            outVal.setSingle(Long.parseLong(s.substring(tab + 1).trim()));
            ctx.write(outKey, outVal);
        }
    }

    /** Correct combiner: output type == input type == map output type, and merge() is associative. */
    public static class StatsCombiner extends Reducer<Text, StatsWritable, Text, StatsWritable> {
        private final StatsWritable acc = new StatsWritable();

        @Override
        protected void reduce(Text key, Iterable<StatsWritable> values, Context ctx) throws IOException, InterruptedException {
            acc.clear();
            for (StatsWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    /**
     * DELIBERATELY WRONG (question 1d). Collapses each mapper's group into ONE pseudo-reading equal to the
     * mapper's local average. The reducer then averages those averages, ignoring how many readings each
     * average came from, so the final mean is wrong whenever two mappers see unequal numbers of readings.
     */
    public static class WrongAvgCombiner extends Reducer<Text, StatsWritable, Text, StatsWritable> {
        private final StatsWritable acc = new StatsWritable();
        private final StatsWritable out = new StatsWritable();

        @Override
        protected void reduce(Text key, Iterable<StatsWritable> values, Context ctx) throws IOException, InterruptedException {
            acc.clear();
            for (StatsWritable v : values) acc.merge(v);
            // optional trace for the report: -D q1.trace.key=city,meter,month records, per map task, the
            // number of readings and their sum as counters (group Q1_TRACE), so the maths of 1d can use real numbers
            String traceKey = ctx.getConfiguration().get("q1.trace.key");
            if (traceKey != null && traceKey.equals(key.toString())) {
                int m = ctx.getTaskAttemptID().getTaskID().getId();
                ctx.getCounter("Q1_TRACE", String.format("map%03d_n", m)).increment(acc.count);
                ctx.getCounter("Q1_TRACE", String.format("map%03d_sum_micro", m)).increment(acc.sum);
            }
            long localAvg = Math.round((double) acc.sum / acc.count);
            out.setSingle(localAvg);          // count = 1: the weight of the mapper's readings is lost
            out.min = acc.min;
            out.max = acc.max;
            ctx.write(key, out);
        }
    }

    public static class StatsReducer extends Reducer<Text, StatsWritable, Text, Text> {
        private final StatsWritable acc = new StatsWritable();
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<StatsWritable> values, Context ctx) throws IOException, InterruptedException {
            acc.clear();
            for (StatsWritable v : values) acc.merge(v);

            double mean = (double) acc.sum / acc.count;
            double variance = (double) acc.sumSq / acc.count - mean * mean;
            if (acc.count == 1) {
                variance = 0.0;                                  // a single reading has sigma = 0 by definition
            } else if (variance < 0.0) {
                ctx.getCounter(Q1Counter.NEGATIVE_VARIANCE_CLAMPED).increment(1);
                variance = 0.0;                                  // floating-point guard (question 1b)
            }
            double stdMicro = Math.sqrt(variance);

            outVal.set(String.format(Locale.ROOT, "%d,%.6f,%.6f,%.9f,%.9f",
                    acc.count, acc.min / 1e6, acc.max / 1e6, mean / 1e6, stdMicro / 1e6));
            ctx.write(key, outVal);
        }
    }

    public static Job build(Configuration conf, Path in, Path out, int reducers,
                            boolean useCombiner, boolean wrongCombiner) throws IOException {
        Job job = Job.getInstance(conf, "q1-B-stats" + (wrongCombiner ? "-WRONG" : useCombiner ? "-combiner" : "-nocombiner"));
        job.setJarByClass(StatsJob.class);
        job.setMapperClass(StatsMapper.class);
        if (wrongCombiner) job.setCombinerClass(WrongAvgCombiner.class);
        else if (useCombiner) job.setCombinerClass(StatsCombiner.class);
        job.setReducerClass(StatsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StatsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
