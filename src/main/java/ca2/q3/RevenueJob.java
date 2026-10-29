package ca2.q3;

import java.io.IOException;

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
 * Job 2 (question 3b): net revenue and net units per (category, order month, product_id).
 *
 * REPLICATED JOIN: products.csv comes from the Distributed Cache and is loaded into a HashMap in Mapper.setup();
 * the category of every line is looked up in map(), nothing about categories is shuffled. A product that is missing
 * from products.csv gets the category UNMAPPED (counter UNMAPPED_LINES).
 *
 * Map     : Job 1 line -> ("category TAB month TAB product_id", NetWritable(net_qty, net_qty * unit_price_E4))
 * Combine : sums NetWritables (associative + commutative, so safe; switch off with -Dq3.combiner=false to compare)
 * Reduce  : sums again -> "category TAB month TAB product_id TAB revenue_E4 TAB units"
 *
 * The revenue of a line belongs to the month of the ORDER (already decided in Job 1).
 */
public class RevenueJob {

    public static class RevenueMapper extends Mapper<LongWritable, Text, Text, NetWritable> {
        private ProductCatalog catalog;
        private final Text outKey = new Text();
        private final NetWritable outVal = new NetWritable();

        @Override
        protected void setup(Context ctx) throws IOException {
            catalog = ProductCatalog.load(ctx);
        }

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            // order_id, line_no, month, product_id, qty, returned, net_qty, unit_price_E4
            String[] f = line.toString().split("\t");
            String product = f[3];
            long net = Long.parseLong(f[6]);
            long priceE4 = Long.parseLong(f[7]);
            if (!catalog.known(product)) ctx.getCounter(Q3Counter.UNMAPPED_LINES).increment(1);
            outKey.set(catalog.category(product) + "\t" + f[2] + "\t" + product);
            outVal.set(net, net * priceE4);
            ctx.write(outKey, outVal);
        }
    }

    public static class SumCombiner extends Reducer<Text, NetWritable, Text, NetWritable> {
        private final NetWritable acc = new NetWritable();

        @Override
        protected void reduce(Text key, Iterable<NetWritable> values, Context ctx) throws IOException, InterruptedException {
            acc.set(0, 0);
            for (NetWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    public static class RevenueReducer extends Reducer<Text, NetWritable, Text, Text> {
        private final NetWritable acc = new NetWritable();
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<NetWritable> values, Context ctx) throws IOException, InterruptedException {
            acc.set(0, 0);
            for (NetWritable v : values) acc.merge(v);
            ctx.getCounter(Q3Counter.GROUPS_WRITTEN).increment(1);
            outVal.set(acc.revenueE4 + "\t" + acc.units);
            ctx.write(key, outVal);
        }
    }

    public static Job build(Configuration conf, Path in, Path products, Path out, int reducers) throws IOException {
        boolean combiner = conf.getBoolean("q3.combiner", true);
        Job job = Job.getInstance(conf, "q3-2-net-revenue" + (combiner ? "-combiner" : "-nocombiner"));
        job.setJarByClass(RevenueJob.class);
        job.addCacheFile(products.toUri());                                // Distributed Cache
        job.setMapperClass(RevenueMapper.class);
        if (combiner) job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(RevenueReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NetWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
