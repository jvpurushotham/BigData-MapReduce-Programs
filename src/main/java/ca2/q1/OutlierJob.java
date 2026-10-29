package ca2.q1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job C (question 1e): for every meter-month, number and percentage of readings with kWh > mean + 2*sigma.
 *
 * REPLICATED (map-side) JOIN: the output of Job B (small: one row per meter-month) is shipped to every
 * mapper through the Distributed Cache and loaded into a HashMap in setup(). The big clean data is only
 * streamed, never shuffled for the join. No reduce-side join is used.
 */
public class OutlierJob {

    public static class OutlierMapper extends Mapper<LongWritable, Text, Text, CountPairWritable> {
        /** "city,meter,month" -> {meanMicro, stdMicro} */
        private final Map<String, double[]> stats = new HashMap<>();
        private final Text outKey = new Text();
        private final CountPairWritable outVal = new CountPairWritable();

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            URI[] cached = ctx.getCacheFiles();
            if (cached == null) throw new IOException("Job B output was not added to the Distributed Cache");
            for (URI uri : cached) {
                Path p = new Path(uri);
                FileSystem fs = p.getFileSystem(conf);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isEmpty()) continue;
                        int tab = line.indexOf('\t');
                        String[] v = line.substring(tab + 1).split(",");   // count,min,max,mean,std (kWh)
                        stats.put(line.substring(0, tab),
                                new double[] { Double.parseDouble(v[3]) * 1e6, Double.parseDouble(v[4]) * 1e6 });
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            int tab = s.indexOf('\t');
            String key = s.substring(0, tab);
            long micro = Long.parseLong(s.substring(tab + 1).trim());
            double[] ms = stats.get(key);
            if (ms == null) {
                ctx.getCounter(Q1Counter.MONTHS_WITHOUT_STATS).increment(1);
                return;
            }
            boolean outlier = micro > ms[0] + 2.0 * ms[1];
            if (outlier) ctx.getCounter(Q1Counter.OUTLIER_READINGS).increment(1);
            outKey.set(key);
            outVal.set(1, outlier ? 1 : 0);
            ctx.write(outKey, outVal);
        }
    }

    public static class OutlierCombiner extends Reducer<Text, CountPairWritable, Text, CountPairWritable> {
        private final CountPairWritable out = new CountPairWritable();

        @Override
        protected void reduce(Text key, Iterable<CountPairWritable> values, Context ctx) throws IOException, InterruptedException {
            long t = 0, o = 0;
            for (CountPairWritable v : values) { t += v.total; o += v.outliers; }
            out.set(t, o);
            ctx.write(key, out);
        }
    }

    public static class OutlierReducer extends Reducer<Text, CountPairWritable, Text, Text> {
        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<CountPairWritable> values, Context ctx) throws IOException, InterruptedException {
            long t = 0, o = 0;
            for (CountPairWritable v : values) { t += v.total; o += v.outliers; }
            outVal.set(String.format(Locale.ROOT, "%d\t%d\t%.4f", t, o, 100.0 * o / t));   // total, outliers, percent
            ctx.write(key, outVal);
        }
    }

    public static Job build(Configuration conf, Path cleanIn, Path statsDir, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q1-C-outliers-replicated-join");
        job.setJarByClass(OutlierJob.class);

        FileSystem fs = statsDir.getFileSystem(conf);
        for (FileStatus st : fs.listStatus(statsDir)) {
            String name = st.getPath().getName();
            if (name.startsWith("part-")) job.addCacheFile(st.getPath().toUri());
        }

        job.setMapperClass(OutlierMapper.class);
        job.setCombinerClass(OutlierCombiner.class);
        job.setReducerClass(OutlierReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountPairWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, cleanIn);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
