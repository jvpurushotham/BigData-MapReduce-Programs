package ca2.q4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job 2: per-IST-date funnel (summarization).
 * map    : "date \t stage" (one non-bot session) -> (date, FunnelWritable) and ("ALL", FunnelWritable)
 * combine: sums the five counters (associative + commutative)
 * reduce : date, sessions, reached_home, reached_product, reached_cart, reached_success,
 *          home_to_product_pct, product_to_cart_pct, cart_to_success_pct, home_to_success_pct
 * The pseudo date "ALL" is the total over all dates (sorts after the dates because 'A' > '2').
 */
public class FunnelJob {

    public static class FunnelMapper extends Mapper<LongWritable, Text, Text, FunnelWritable> {
        private final Text k = new Text();
        private final Text all = new Text("ALL");
        private final FunnelWritable v = new FunnelWritable();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            k.set(f[0]);
            v.setSession(Integer.parseInt(f[1]));
            ctx.write(k, v);
            ctx.write(all, v);
        }
    }

    public static class SumCombiner extends Reducer<Text, FunnelWritable, Text, FunnelWritable> {
        private final FunnelWritable sum = new FunnelWritable();

        @Override
        protected void reduce(Text key, Iterable<FunnelWritable> values, Context ctx) throws IOException, InterruptedException {
            sum.clear();
            for (FunnelWritable v : values) sum.add(v);
            ctx.write(key, sum);
        }
    }

    public static class FunnelReducer extends Reducer<Text, FunnelWritable, NullWritable, Text> {
        private final FunnelWritable sum = new FunnelWritable();
        private final Text out = new Text();

        /** num/den as a percentage with 2 decimals, HALF_UP, integer arithmetic only ("NA" when den = 0). */
        static String pct(long num, long den) {
            if (den == 0) return "NA";
            long hundredths = (2 * num * 10_000L + den) / (2 * den);
            return (hundredths / 100) + "." + String.format("%02d", hundredths % 100);
        }

        @Override
        protected void reduce(Text key, Iterable<FunnelWritable> values, Context ctx) throws IOException, InterruptedException {
            sum.clear();
            for (FunnelWritable v : values) sum.add(v);
            long[] c = sum.c;
            out.set(key + "\t" + c[0] + "\t" + c[1] + "\t" + c[2] + "\t" + c[3] + "\t" + c[4]
                    + "\t" + pct(c[2], c[1]) + "\t" + pct(c[3], c[2]) + "\t" + pct(c[4], c[3]) + "\t" + pct(c[4], c[1]));
            ctx.write(NullWritable.get(), out);
            ctx.getCounter(Q4Counter.FUNNEL_ROWS_WRITTEN).increment(1);
        }
    }

    public static Job build(Configuration conf, Path in, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q4-2-funnel");
        job.setJarByClass(FunnelJob.class);
        job.setMapperClass(FunnelMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(FunnelReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FunnelWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
