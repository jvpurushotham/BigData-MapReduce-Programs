package ca2.q2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
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
 * Job 3 (question 2e): phrase query answered from the INDEX ONLY (the raw corpus is never read).
 *
 * Phrase  : -Dphrase="big data analytics"  (2 or 3 words, tokenised exactly like the corpus).
 * Map     : index line "term TAB df TAB doc:tf:tfidf:[p,p,..] ..."  ->  for every phrase word found,
 *           (doc_id, "k|p,p,p")  where k is the word's offset inside the phrase (0, 1, 2).
 *           Lines whose term is not a phrase word are skipped immediately.
 * Reduce  : (doc_id, all (k, positions) parts) -> count start positions p such that word k sits at p + k for every k.
 *           One reducer collects the matching documents and emits them ranked by number of occurrences
 *           (descending), then doc_id (ascending).  The result set is tiny, so buffering it is fine.
 *           Documents that contain every phrase word but never adjacently go to the side file all_words_not_adjacent.
 *
 * Only words that are in the MAIN index can be searched: stop-words, corpus-stop terms and rare (df = 1) terms were
 * deliberately kept out of it by Job 2, so a phrase containing one of them finds nothing.
 */
public class PhraseJob {

    public static List<String> phraseTerms(Configuration conf) {
        String phrase = conf.get("phrase", "");
        return Analyzer.tokens(phrase);
    }

    // ------------------------------------------------------------------------------------------------ mapper
    public static class PhraseMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, List<Integer>> wanted = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            List<String> terms = phraseTerms(ctx.getConfiguration());
            for (int k = 0; k < terms.size(); k++) {
                wanted.computeIfAbsent(terms.get(k), x -> new ArrayList<>()).add(k);
            }
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            String s = line.toString();
            int t1 = s.indexOf('\t');
            if (t1 < 0) return;
            String term = s.substring(0, t1);
            List<Integer> ks = wanted.get(term);
            if (ks == null) return;                                   // not a phrase word

            ctx.getCounter(Q2Counter.PHRASE_INDEX_LINES_READ).increment(1);
            ctx.getCounter("Q2_PHRASE_TERMS", term).increment(1);     // lets the driver report words missing from the index

            int t2 = s.indexOf('\t', t1 + 1);
            String postings = s.substring(t2 + 1);
            int i = 0, len = postings.length();
            while (i < len) {
                int end = postings.indexOf(' ', i);
                if (end < 0) end = len;
                String p = postings.substring(i, end);
                int c1 = p.indexOf(':');
                int c2 = p.indexOf(':', c1 + 1);
                int c3 = p.indexOf(':', c2 + 1);
                String docId = p.substring(0, c1);
                String positions = p.substring(c3 + 2, p.length() - 1);   // strip the surrounding [ ]
                outKey.set(docId);
                for (int k : ks) {
                    outVal.set(k + "|" + positions);
                    ctx.write(outKey, outVal);
                }
                i = end + 1;
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ reducer
    public static class PhraseReducer extends Reducer<Text, Text, Text, Text> {
        private int words;
        private MultipleOutputs<Text, Text> mos;

        private static final class Hit {
            final String doc;
            final int count;
            Hit(String doc, int count) { this.doc = doc; this.count = count; }
        }
        private final List<Hit> hits = new ArrayList<>();

        @Override
        protected void setup(Context ctx) {
            words = phraseTerms(ctx.getConfiguration()).size();
            mos = new MultipleOutputs<>(ctx);
        }

        @Override
        protected void reduce(Text doc, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            List<Set<Integer>> at = new ArrayList<>();
            for (int k = 0; k < words; k++) at.add(null);
            int[] first = null;

            for (Text v : values) {
                String s = v.toString();
                int bar = s.indexOf('|');
                int k = Integer.parseInt(s.substring(0, bar));
                String[] ps = s.substring(bar + 1).split(",");
                Set<Integer> set = new HashSet<>();
                int[] arr = new int[ps.length];
                for (int i = 0; i < ps.length; i++) { arr[i] = Integer.parseInt(ps[i].trim()); set.add(arr[i]); }
                at.set(k, set);
                if (k == 0) first = arr;
            }
            for (Set<Integer> s : at) if (s == null) return;           // some phrase word is not in this document

            ctx.getCounter(Q2Counter.PHRASE_DOCS_WITH_ALL_WORDS).increment(1);
            int count = 0;
            for (int p : first) {
                boolean ok = true;
                for (int k = 1; k < words && ok; k++) ok = at.get(k).contains(p + k);
                if (ok) count++;
            }
            if (count > 0) {
                hits.add(new Hit(doc.toString(), count));
                ctx.getCounter(Q2Counter.PHRASE_DOCS_MATCHED).increment(1);
                ctx.getCounter(Q2Counter.PHRASE_OCCURRENCES).increment(count);
            } else {
                ctx.getCounter(Q2Counter.PHRASE_DOCS_ALL_WORDS_NOT_ADJACENT).increment(1);
                mos.write(doc, new Text("all words present, never adjacent"), "all_words_not_adjacent");
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            Collections.sort(hits, (a, b) -> a.count != b.count ? Integer.compare(b.count, a.count) : a.doc.compareTo(b.doc));
            Text k = new Text(), v = new Text();
            for (Hit h : hits) {
                k.set(h.doc);
                v.set(Integer.toString(h.count));
                ctx.write(k, v);
            }
            mos.close();
        }
    }

    public static Job build(Configuration conf, Path indexGlob, Path out) throws IOException {
        Job job = Job.getInstance(conf, "q2-3-phrase-" + conf.get("phrase", "").replace(' ', '_'));
        job.setJarByClass(PhraseJob.class);
        job.setMapperClass(PhraseMapper.class);
        job.setReducerClass(PhraseReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, indexGlob);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
