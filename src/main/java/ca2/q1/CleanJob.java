package ca2.q1;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

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
 * Job A (question 1a): validate and de-duplicate.
 *
 * Map   : (offset, line) -> ("meter_id|reading_ts", "city,yyyy-MM,microKwh") for valid lines only.
 * Reduce: all copies of the same meter+timestamp arrive together, so exact duplicates -- even those
 *         that were read by two different mappers from two different files -- meet here. Keep one,
 *         count the rest in DUPLICATE_READING. Output ("city,meter_id,yyyy-MM", microKwh).
 *
 * De-duplication has to be a reduce-side step: a mapper only sees its own split.
 */
public class CleanJob {

    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withResolverStyle(ResolverStyle.STRICT);

    public static class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            ctx.getCounter(Q1Counter.RECORDS_READ).increment(1);
            String s = line.toString().trim();
            if (s.isEmpty()) {
                ctx.getCounter(Q1Counter.REJECTED_EMPTY_FIELD).increment(1);
                return;
            }
            if (s.startsWith("meter_id")) {
                ctx.getCounter(Q1Counter.HEADER_SKIPPED).increment(1);
                return;
            }
            String[] f = s.split(",", -1);
            if (f.length != 4) {
                ctx.getCounter(Q1Counter.REJECTED_FIELD_COUNT).increment(1);
                return;
            }
            String meter = f[0].trim(), city = f[1].trim(), ts = f[2].trim(), kwhStr = f[3].trim();
            if (meter.isEmpty() || city.isEmpty() || ts.isEmpty() || kwhStr.isEmpty()) {
                ctx.getCounter(Q1Counter.REJECTED_EMPTY_FIELD).increment(1);
                return;
            }
            try {
                TS_FORMAT.parse(ts);          // strict: rejects 14/03/2026 18:30, 2026-02-30 ..., 24:00:00 ...
            } catch (Exception e) {
                ctx.getCounter(Q1Counter.REJECTED_BAD_TIMESTAMP).increment(1);
                return;
            }
            long micro;
            BigDecimal kwh;
            try {
                kwh = new BigDecimal(kwhStr);
                micro = kwh.movePointRight(6).setScale(0, RoundingMode.HALF_UP).longValueExact();
            } catch (NumberFormatException | ArithmeticException e) {
                ctx.getCounter(Q1Counter.REJECTED_BAD_KWH).increment(1);
                return;
            }
            if (kwh.signum() < 0) {
                ctx.getCounter(Q1Counter.REJECTED_NEGATIVE_KWH).increment(1);
                return;
            }
            outKey.set(meter + "|" + ts);
            outVal.set(city + "," + ts.substring(0, 7) + "," + micro);   // month is taken from the timestamp itself
            ctx.write(outKey, outVal);
        }
    }

    public static class DedupReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            String chosen = null;
            long copies = 0;
            for (Text v : values) {
                copies++;
                String s = v.toString();
                // deterministic choice if copies ever disagree: lexicographically smallest
                if (chosen == null || s.compareTo(chosen) < 0) chosen = s;
            }
            if (copies > 1) ctx.getCounter(Q1Counter.DUPLICATE_READING).increment(copies - 1);
            ctx.getCounter(Q1Counter.UNIQUE_READINGS).increment(1);

            String meter = key.toString().substring(0, key.toString().indexOf('|'));
            String[] p = chosen.split(",");                  // city, month, micro
            outKey.set(p[0] + "," + meter + "," + p[1]);
            outVal.set(p[2]);
            ctx.write(outKey, outVal);
        }
    }

    public static Job build(Configuration conf, Path in, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q1-A-clean-dedup");
        job.setJarByClass(CleanJob.class);
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(DedupReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
