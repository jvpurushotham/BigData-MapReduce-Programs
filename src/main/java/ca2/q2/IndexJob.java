package ca2.q2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Job 2 (questions 2a, 2c, 2d): positional inverted index with TF-IDF.
 *
 * Map    : (offset, "doc_id TAB title TAB body")  ->  (term, PostingWritable(doc_id, tf, positions))
 *          One posting per (term, document).  tf = 3 * (title occurrences) + (body occurrences).
 *          Stop-words are read from the Distributed Cache in setup().
 * Reduce : (term, postings)  ->  term TAB df TAB postings          with  tfidf = tf * log10(N / df)
 *            df >  0.4 N  -> MultipleOutputs file  corpus_stop
 *            df == 1      -> MultipleOutputs file  rare
 *            otherwise    -> main index (part-r-*)
 *
 * COMBINER: not applicable here -- see the explanation in the report text printed by Q2Driver / README_Q2.md.
 *   * Every (term, doc) pair is produced exactly once, inside one mapper, because a document is one input line
 *     and the mapper already aggregates tf and positions per document (in-mapper combining). There is nothing
 *     left to merge, so a combiner would see only singleton groups and cannot reduce records or bytes.
 *   * df, N-dependent tf-idf, the corpus_stop / rare decision and the final ordering all need EVERY posting of the
 *     term, so none of them can be computed from partial results.
 *   * The only merge that is legal (concatenating postings into a list) needs a different map-output type
 *     (list instead of single posting), saves no bytes, and moves the sorting cost without removing it.
 */
public class IndexJob {

    // ------------------------------------------------------------------------------------------------ mapper
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, PostingWritable> {

        /** Accumulates title count, body count and positions of one term inside one document. */
        private static final class Acc {
            int title;
            int body;
            int[] pos = new int[4];
            int n;
            void add(int p) {
                if (n == pos.length) pos = java.util.Arrays.copyOf(pos, n * 2);
                pos[n++] = p;
            }
        }

        private final Set<String> stop = new HashSet<>();
        private boolean originalPositions;
        private final Text outKey = new Text();
        private final PostingWritable outVal = new PostingWritable();

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            PostingWritable.configure(conf);
            originalPositions = "original".equalsIgnoreCase(conf.get("q2.positions", "filtered"));
            URI[] cached = ctx.getCacheFiles();
            if (cached == null || cached.length == 0) throw new IOException("stopwords.txt was not added to the Distributed Cache");
            for (URI uri : cached) {
                Path p = new Path(uri);
                FileSystem fs = p.getFileSystem(conf);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String w = line.trim().toLowerCase(Locale.ROOT);
                        if (w.isEmpty() || w.startsWith("#")) continue;
                        if (stop.add(w)) ctx.getCounter(Q2Counter.STOPWORDS_LOADED).increment(1);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            if (s.trim().isEmpty()) {
                ctx.getCounter(Q2Counter.BLANK_LINES).increment(1);
                return;
            }
            Analyzer.Doc d = Analyzer.parse(s);
            if (d == null) {
                ctx.getCounter(Q2Counter.MALFORMED_LINES).increment(1);
                return;
            }
            if (d.id.equalsIgnoreCase("doc_id")) {
                ctx.getCounter(Q2Counter.HEADER_SKIPPED).increment(1);
                return;
            }
            if (d.extraFields) ctx.getCounter(Q2Counter.EXTRA_FIELDS_MERGED).increment(1);
            if (d.title.trim().isEmpty()) ctx.getCounter(Q2Counter.EMPTY_TITLE_DOCS).increment(1);
            if (d.body.trim().isEmpty()) ctx.getCounter(Q2Counter.EMPTY_BODY_DOCS).increment(1);

            Map<String, Acc> terms = new LinkedHashMap<>();
            int pos = 0;                                    // title and body form ONE stream, starting at 0
            for (int part = 0; part < 2; part++) {
                boolean inTitle = (part == 0);
                for (String t : Analyzer.tokens(inTitle ? d.title : d.body)) {
                    ctx.getCounter(Q2Counter.TOKENS_TOTAL).increment(1);
                    if (t.length() < 2) {
                        ctx.getCounter(Q2Counter.TOKENS_DROPPED_SHORT).increment(1);
                        if (originalPositions) pos++;
                        continue;
                    }
                    if (stop.contains(t)) {
                        ctx.getCounter(Q2Counter.TOKENS_DROPPED_STOPWORD).increment(1);
                        if (originalPositions) pos++;
                        continue;
                    }
                    ctx.getCounter(Q2Counter.TOKENS_KEPT).increment(1);
                    Acc a = terms.get(t);
                    if (a == null) { a = new Acc(); terms.put(t, a); }
                    if (inTitle) a.title++; else a.body++;
                    a.add(pos);
                    pos++;
                }
            }
            if (terms.isEmpty()) ctx.getCounter(Q2Counter.DOCS_WITHOUT_INDEXABLE_TOKENS).increment(1);

            for (Map.Entry<String, Acc> e : terms.entrySet()) {
                Acc a = e.getValue();
                if (a.title > 0 && a.body == 0) ctx.getCounter(Q2Counter.TERM_IN_TITLE_ONLY_OF_DOC).increment(1);
                outKey.set(e.getKey());
                outVal.set(d.id, 3 * a.title + a.body, a.pos, a.n);
                ctx.write(outKey, outVal);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ reducer
    public static class IndexReducer extends Reducer<Text, PostingWritable, Text, Text> {

        private MultipleOutputs<Text, Text> mos;
        private long n;
        private final Text outVal = new Text();

        /** A posting together with its rounded tf-idf, ready to be sorted. */
        private static final class Scored {
            final PostingWritable p;
            final BigDecimal tfidf;
            Scored(PostingWritable p, BigDecimal tfidf) { this.p = p; this.tfidf = tfidf; }
        }

        @Override
        protected void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            PostingWritable.configure(conf);
            n = conf.getLong("q2.N", -1);
            if (n <= 0) throw new IllegalStateException("q2.N was not passed by the driver");
            mos = new MultipleOutputs<>(ctx);
        }

        @Override
        protected void reduce(Text term, Iterable<PostingWritable> values, Context ctx) throws IOException, InterruptedException {
            List<PostingWritable> postings = new ArrayList<>();
            for (PostingWritable v : values) postings.add(v.copy());      // private copies: Hadoop re-uses v
            int df = postings.size();                                      // one posting per document

            double idf = Math.log10((double) n / df);
            List<Scored> scored = new ArrayList<>(df);
            for (PostingWritable p : postings) {
                // exact decimal expansion of the double, then HALF_UP to 4 places (same rule in the Python checker)
                BigDecimal t = new BigDecimal(p.tf * idf).setScale(4, RoundingMode.HALF_UP);
                scored.add(new Scored(p, t));
            }
            Collections.sort(scored, (a, b) -> {
                int c = b.tfidf.compareTo(a.tfidf);                        // tf-idf descending
                return c != 0 ? c : a.p.docId.compareTo(b.p.docId);        // then doc_id ascending
            });

            StringBuilder sb = new StringBuilder();
            sb.append(df).append('\t');
            boolean first = true;
            for (Scored sc : scored) {
                if (!first) sb.append(' ');
                first = false;
                sb.append(sc.p.docId).append(':').append(sc.p.tf).append(':').append(sc.tfidf.toPlainString()).append(":[");
                for (int i = 0; i < sc.p.n; i++) {
                    if (i > 0) sb.append(',');
                    sb.append(sc.p.positions[i]);
                }
                sb.append(']');
            }
            outVal.set(sb.toString());

            ctx.getCounter(Q2Counter.POSTINGS_WRITTEN).increment(df);
            if (df > 0.4 * n) {                                            // corpus stop-word (checked first)
                ctx.getCounter(Q2Counter.TERMS_CORPUS_STOP).increment(1);
                mos.write(term, outVal, "corpus_stop");
            } else if (df == 1) {                                          // rare term
                ctx.getCounter(Q2Counter.TERMS_RARE).increment(1);
                mos.write(term, outVal, "rare");
            } else {
                ctx.getCounter(Q2Counter.TERMS_INDEXED).increment(1);
                ctx.write(term, outVal);
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static Job build(Configuration conf, Path corpus, Path stopwords, Path out, int reducers) throws IOException {
        Job job = Job.getInstance(conf, "q2-2-index-delta-" + conf.getBoolean("q2.delta", true));
        job.setJarByClass(IndexJob.class);
        job.addCacheFile(stopwords.toUri());                               // Distributed Cache
        job.setMapperClass(IndexMapper.class);
        // no Combiner on purpose: see class comment
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PostingWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.addInputPath(job, corpus);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
