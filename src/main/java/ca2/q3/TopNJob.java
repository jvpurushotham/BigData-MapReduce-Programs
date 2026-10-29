package ca2.q3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job 3 (question 3c): Top-N products per (category, month) by net revenue.
 *
 * Mapper  : keeps, per group, a bounded TreeSet of at most N entries (N = 5) ordered by EntryWritable.RANK and emits
 *           the survivors in cleanup(). Memory is O(groups x N): a product that is pushed out of the set can never
 *           come back, because a better entry is already in it. (Safe HERE because Job 2 already produced exactly one
 *           row per (category, month, product): every row carries the product's complete revenue.)
 * Partitioner : CategoryPartitioner routes every group of one category to the same reducer.
 * Reducer : receives one (category, month) group at a time, keeps the best N in a bounded TreeSet (streaming, no list of
 *           all products), looks the product name up in the Distributed Cache and writes
 *           category, month, rank, product_id, product_name, net_revenue, net_units  (TAB separated).
 *
 * Experiment switches:  -Dq3.topn.n=5 (the N) and -Dq3.topn.bounded=false (mapper keeps and emits EVERYTHING, to show
 * how much shuffle the bounded structure saves; only for measuring, it is no longer O(groups x N) memory).
 */
public class TopNJob {

    public static class TopMapper extends Mapper<LongWritable, Text, GroupKey, EntryWritable> {
        private int n;
        private boolean bounded;
        private final Map<String, TreeSet<EntryWritable>> groups = new HashMap<>();

        @Override
        protected void setup(Context ctx) {
            n = ctx.getConfiguration().getInt("q3.topn.n", 5);
            bounded = ctx.getConfiguration().getBoolean("q3.topn.bounded", true);
        }

        @Override
        protected void map(LongWritable off, Text line, Context ctx) {
            // category, month, product_id, revenue_E4, units
            String[] f = line.toString().split("\t");
            ctx.getCounter(Q3Counter.TOPN_INPUT_GROUP_ROWS).increment(1);
            String g = f[0] + "\t" + f[1];
            TreeSet<EntryWritable> set = groups.get(g);
            if (set == null) { set = new TreeSet<>(EntryWritable.RANK); groups.put(g, set); }
            set.add(new EntryWritable(f[2], Long.parseLong(f[3]), Long.parseLong(f[4])));
            if (bounded && set.size() > n) {
                set.pollLast();                                       // worst of the n+1: gone for good
                ctx.getCounter(Q3Counter.TOPN_MAPPER_EVICTIONS).increment(1);
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            GroupKey key = new GroupKey();
            for (Map.Entry<String, TreeSet<EntryWritable>> e : groups.entrySet()) {
                int tab = e.getKey().indexOf('\t');
                key.set(e.getKey().substring(0, tab), e.getKey().substring(tab + 1));
                for (EntryWritable en : e.getValue()) {
                    ctx.write(key, en);
                    ctx.getCounter(Q3Counter.TOPN_MAPPER_ENTRIES_EMITTED).increment(1);
                }
            }
        }
    }

    /** All groups of one category go to the same reducer: partition = hash(category) mod #reducers. */
    public static class CategoryPartitioner extends Partitioner<GroupKey, EntryWritable> {
        @Override
        public int getPartition(GroupKey key, EntryWritable value, int numPartitions) {
            return (key.category.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class TopReducer extends Reducer<GroupKey, EntryWritable, NullWritable, Text> {
        private int n;
        private ProductCatalog catalog;
        private final Text out = new Text();

        @Override
        protected void setup(Context ctx) throws IOException {
            n = ctx.getConfiguration().getInt("q3.topn.n", 5);
            catalog = ProductCatalog.load(ctx);
        }

        @Override
        protected void reduce(GroupKey key, Iterable<EntryWritable> values, Context ctx) throws IOException, InterruptedException {
            TreeSet<EntryWritable> top = new TreeSet<>(EntryWritable.RANK);
            for (EntryWritable v : values) {
                top.add(v.copy());                                   // Hadoop re-uses v
                if (top.size() > n) top.pollLast();
            }
            ctx.getCounter(Q3Counter.TOPN_GROUPS).increment(1);
            ctx.getCounter("Q3_PARTITIONS", "reducer" + String.format("%02d", ctx.getTaskAttemptID().getTaskID().getId())).increment(1);
            int rank = 0;
            for (EntryWritable e : top) {
                rank++;
                out.set(key.category + "\t" + key.month + "\t" + rank + "\t" + e.productId + "\t" + catalog.name(e.productId)
                        + "\t" + Money.format(e.revenueE4) + "\t" + e.units);
                ctx.write(NullWritable.get(), out);
                ctx.getCounter(Q3Counter.TOPN_ROWS_WRITTEN).increment(1);
            }
        }
    }

    public static Job build(Configuration conf, Path in, Path products, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q3-3-top" + conf.getInt("q3.topn.n", 5)
                + (conf.getBoolean("q3.topn.bounded", true) ? "" : "-UNBOUNDED"));
        job.setJarByClass(TopNJob.class);
        job.addCacheFile(products.toUri());
        job.setMapperClass(TopMapper.class);
        job.setPartitionerClass(CategoryPartitioner.class);
        job.setReducerClass(TopReducer.class);
        job.setMapOutputKeyClass(GroupKey.class);
        job.setMapOutputValueClass(EntryWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
