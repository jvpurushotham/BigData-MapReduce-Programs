package ca2.q4;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Job 1: sessionization with SECONDARY SORT.
 * <pre>
 *   map    : line "user|ts|url|referrer|agent"  ->  (UserTsKey(user, ts, url), NullWritable)
 *   shuffle: Partitioner on user_id, SortComparator (user, ts[, url]), GroupingComparator on user_id
 *   reduce : ONE call per user, events arrive in time order; a small state machine cuts sessions while streaming.
 * </pre>
 * Output  : sessions/part-r-* , bots/part-r-* (MultipleOutputs), funnel_input/part-r-* (MultipleOutputs: date, stage).
 * The reducer NEVER keeps the events of a user: its only state is the open session (counters, 2 timestamps, entry/exit
 * url, the funnel stage and the set of DISTINCT urls of the open session, cleared when the session closes).
 */
public class SessionJob {

    public static class ClickMapper extends Mapper<LongWritable, Text, UserTsKey, NullWritable> {
        private final UserTsKey key = new UserTsKey();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            if (s.trim().isEmpty()) return;
            ctx.getCounter(Q4Counter.EVENTS_READ).increment(1);
            String[] f = s.split("\\|", 5);          // limit 5: a '|' inside the user agent cannot shift the fields
            if (f.length < 5) { ctx.getCounter(Q4Counter.BAD_FIELD_COUNT).increment(1); return; }
            String user = f[0].trim();
            if (user.isEmpty()) { ctx.getCounter(Q4Counter.BAD_USER).increment(1); return; }
            long ts;
            try {
                ts = Long.parseLong(f[1].trim());
            } catch (NumberFormatException e) { ctx.getCounter(Q4Counter.BAD_TIMESTAMP).increment(1); return; }
            if (ts < 0) { ctx.getCounter(Q4Counter.BAD_TIMESTAMP).increment(1); return; }
            String url = f[2].trim();
            if (url.isEmpty()) { ctx.getCounter(Q4Counter.BAD_URL).increment(1); return; }
            key.set(user, ts, url);
            ctx.write(key, NullWritable.get());
        }
    }

    public static class SessionReducer extends Reducer<UserTsKey, NullWritable, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;
        private final Text out = new Text();
        private long gapMs, botEvents, botAvgGapMs;

        // ---- state of the OPEN session only ----
        private boolean open;
        private String user;
        private long seq, startTs, lastTs, count;
        private int stage;
        private boolean cartBeforeProduct;
        private String entry, exit;
        private final Set<String> distinct = new HashSet<>();

        @Override
        protected void setup(Context ctx) {
            mos = new MultipleOutputs<>(ctx);
            Configuration c = ctx.getConfiguration();
            gapMs = c.getLong("q4.gap.minutes", 30) * 60_000L;
            botEvents = c.getLong("q4.bot.events", 500);
            botAvgGapMs = c.getLong("q4.bot.avggap.ms", 1000);
        }

        @Override
        protected void reduce(UserTsKey key, Iterable<NullWritable> values, Context ctx) throws IOException, InterruptedException {
            user = key.getUser();
            ctx.getCounter(Q4Counter.USERS).increment(1);
            seq = 0;
            open = false;
            for (NullWritable ignored : values) {          // STREAM: Hadoop refreshes `key` for every value
                long ts = key.getTs();
                String url = key.getUrl();
                if (open) {
                    long gap = ts - lastTs;
                    boolean byGap = gap > gapMs;                               // strictly greater
                    boolean byDate = Ist.day(ts) != Ist.day(lastTs);           // calendar date change in IST
                    if (gap == 0) ctx.getCounter(Q4Counter.EVENTS_SAME_TS_AS_PREVIOUS).increment(1);
                    if (byGap || byDate) {
                        ctx.getCounter(byGap ? Q4Counter.SPLIT_BY_GAP : Q4Counter.SPLIT_BY_MIDNIGHT).increment(1);
                        close(ctx);
                    } else if (gap == gapMs) {
                        ctx.getCounter(Q4Counter.GAP_EXACTLY_30_MIN_KEPT).increment(1);
                    }
                }
                if (!open) {
                    open = true;
                    seq++;
                    startTs = ts;
                    count = 0;
                    stage = 0;
                    cartBeforeProduct = false;
                    entry = url;
                    distinct.clear();
                }
                count++;
                lastTs = ts;
                exit = url;
                distinct.add(url);
                advance(path(url));
            }
            if (open) close(ctx);
        }

        /** URL without query string / fragment. */
        static String path(String url) {
            int q = url.indexOf('?');
            int h = url.indexOf('#');
            int cut = q < 0 ? h : (h < 0 ? q : Math.min(q, h));
            return cut < 0 ? url : url.substring(0, cut);
        }

        /** Greedy in-order matching: a stage is reached only when the previous one has already been reached. */
        private void advance(String p) {
            if (stage < 2 && p.equals("/cart")) cartBeforeProduct = true;     // /cart before any /product: not a conversion
            switch (stage) {
                case 0: if (p.equals("/home")) stage = 1; break;
                case 1: if (p.startsWith("/product/") && p.length() > 9) stage = 2; break;
                case 2: if (p.equals("/cart")) stage = 3; break;
                case 3: if (p.equals("/checkout/success")) stage = 4; break;
                default: break;
            }
        }

        private void close(Context ctx) throws IOException, InterruptedException {
            open = false;
            long dur = lastTs - startTs;
            boolean many = count > botEvents;
            boolean fast = count >= 2 && dur < botAvgGapMs * (count - 1);     // average gap = dur / (count - 1) < threshold
            boolean bot = many || fast;
            ctx.getCounter(Q4Counter.SESSIONS).increment(1);
            StringBuilder sb = new StringBuilder();
            sb.append(user).append('\t').append(seq).append('\t').append(Ist.iso(startTs)).append('\t').append(Ist.iso(lastTs))
              .append('\t').append(Ist.seconds(dur)).append('\t').append(count).append('\t').append(distinct.size())
              .append('\t').append(entry).append('\t').append(exit);
            if (bot) {
                ctx.getCounter(Q4Counter.SESSIONS_BOT).increment(1);
                ctx.getCounter(many && fast ? Q4Counter.BOT_BOTH_REASONS : many ? Q4Counter.BOT_TOO_MANY_EVENTS : Q4Counter.BOT_FAST_AVG_GAP).increment(1);
                sb.append('\t').append(many && fast ? "EVENTS+AVG_GAP" : many ? "EVENTS>" + botEvents : "AVG_GAP<" + botAvgGapMs + "ms");
                out.set(sb.toString());
                mos.write(NullWritable.get(), out, "bots/part");               // excluded from the funnel
                return;
            }
            ctx.getCounter(Q4Counter.SESSIONS_NORMAL).increment(1);
            out.set(sb.toString());
            mos.write(NullWritable.get(), out, "sessions/part");
            out.set(Ist.date(Ist.day(startTs)) + "\t" + stage);
            mos.write(NullWritable.get(), out, "funnel_input/part");
            ctx.getCounter(Q4Counter.FUNNEL_RECORDS).increment(1);
            if (stage >= 1) ctx.getCounter(Q4Counter.FUNNEL_STAGE_HOME).increment(1);
            if (stage >= 2) ctx.getCounter(Q4Counter.FUNNEL_STAGE_PRODUCT).increment(1);
            if (stage >= 3) ctx.getCounter(Q4Counter.FUNNEL_STAGE_CART).increment(1);
            if (stage >= 4) ctx.getCounter(Q4Counter.FUNNEL_STAGE_SUCCESS).increment(1);
            if (cartBeforeProduct) ctx.getCounter(Q4Counter.CART_BEFORE_PRODUCT_SESSIONS).increment(1);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static Job build(Configuration conf, Path in, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q4-1-sessions");
        job.setJarByClass(SessionJob.class);
        job.setMapperClass(ClickMapper.class);
        job.setReducerClass(SessionReducer.class);
        job.setMapOutputKeyClass(UserTsKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setPartitionerClass(UserPartitioner.class);
        job.setSortComparatorClass(UserTsSortComparator.class);
        job.setGroupingComparatorClass(UserGroupingComparator.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);   // no empty part-r-* files: all output goes through MultipleOutputs
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
