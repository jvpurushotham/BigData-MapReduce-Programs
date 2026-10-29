package ca2.q5;

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
 * Stage C: summary file. Reads the "summary_feed" lines "category \t order_inr_E4 \t net_paid_inr_E4 \t settled_inr_E4"
 * written by stages A and B and writes "category \t count \t order_amount_inr \t net_paid_inr \t settled_inr" (Combiner: plain sums).
 */
public class SummaryJob {

    public static class FeedMapper extends Mapper<LongWritable, Text, Text, SummaryWritable> {
        private final Text k = new Text();
        private final SummaryWritable v = new SummaryWritable();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] f = line.toString().split("\t");
            k.set(f[0]);
            v.set(1, Long.parseLong(f[1]), Long.parseLong(f[2]), Long.parseLong(f[3]));
            ctx.write(k, v);
        }
    }

    public static class SumCombiner extends Reducer<Text, SummaryWritable, Text, SummaryWritable> {
        private final SummaryWritable sum = new SummaryWritable();

        @Override
        protected void reduce(Text key, Iterable<SummaryWritable> values, Context ctx) throws IOException, InterruptedException {
            sum.clear();
            for (SummaryWritable v : values) sum.add(v);
            ctx.write(key, sum);
        }
    }

    public static class SummaryReducer extends Reducer<Text, SummaryWritable, NullWritable, Text> {
        private final SummaryWritable sum = new SummaryWritable();
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<SummaryWritable> values, Context ctx) throws IOException, InterruptedException {
            sum.clear();
            for (SummaryWritable v : values) sum.add(v);
            out.set(key + "\t" + sum.count + "\t" + Money.format(sum.orderE4) + "\t" + Money.format(sum.netE4) + "\t" + Money.format(sum.settledE4));
            ctx.write(NullWritable.get(), out);
            ctx.getCounter(Q5Counter.SUMMARY_ROWS_WRITTEN).increment(1);
        }
    }

    public static Job build(Configuration conf, Path[] feeds, Path out) throws IOException {
        Job job = Job.getInstance(conf, "q5-3-summary");
        job.setJarByClass(SummaryJob.class);
        job.setMapperClass(FeedMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(SummaryReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SummaryWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        for (Path p : feeds) FileInputFormat.addInputPath(job, p);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
