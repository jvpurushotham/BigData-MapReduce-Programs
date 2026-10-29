package ca2.q2;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Single driver for Question 2.   Job 1 (count N) -> Job 2 (index) -> Job 3 (phrase search).
 *
 * Usage : hadoop jar ca2.jar ca2.q2.Q2Driver [-D key=value ...] <corpus_dir> <stopwords_file> <output_dir>
 *
 * Optional -D switches
 *   phrase="big data analytics"   phrase query of 2-3 words (Job 3 is skipped when it is absent)
 *   q2.only.phrase=true           do NOT rebuild the index; only run Job 3 on <output_dir>/index (different phrases, viva)
 *   q2.delta=true|false           delta (gap) encoding of positions in the shuffle                (default true)
 *   q2.positions=filtered|original  filtered: positions count only the tokens that are indexed (default)
 *                                   original: dropped short tokens and stop-words still occupy a position
 *   q2.reducers=N                 reducers of Job 2                                               (default 2)
 *   q2.phrase.out=<dir>           where Job 3 writes                                              (default <output>/phrase)
 *
 * Output layout : <output>/index/part-r-*  (main index) + corpus_stop-r-* + rare-r-*   and   <output>/phrase
 * Exit status   : non-zero if any stage fails.
 */
public class Q2Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Q2Driver [-D key=value] <corpus_dir> <stopwords_file> <output_dir>");
            return 2;
        }
        Configuration conf = getConf();
        Path corpus = new Path(args[0]);
        Path stopwords = new Path(args[1]);
        Path out = new Path(args[2]);
        Path index = new Path(out, "index");
        Path phraseOut = new Path(conf.get("q2.phrase.out", new Path(out, "phrase").toString()));
        boolean onlyPhrase = conf.getBoolean("q2.only.phrase", false);
        int reducers = conf.getInt("q2.reducers", 2);
        String phrase = conf.get("phrase");

        FileSystem fs = out.getFileSystem(conf);

        if (phrase != null) {
            List<String> terms = PhraseJob.phraseTerms(conf);
            if (terms.size() < 2 || terms.size() > 3) {
                System.err.println("[Q2] the phrase must contain 2 or 3 words, got " + terms.size() + ": " + terms);
                return 2;
            }
        }

        if (!onlyPhrase) {
            fs.delete(out, true);

            // ---- Job 1 : count documents (custom counter) --------------------------------------------
            Job j1 = CountDocsJob.build(conf, corpus);
            if (!j1.waitForCompletion(true)) return fail("Job 1 (count N) failed");
            long n = j1.getCounters().findCounter(Q2Counter.DOCS).getValue();
            System.out.println("[Q2] ---- Job 1 ----");
            printDocCounters(j1.getCounters());
            if (n <= 0) return fail("N = 0: no valid documents in " + corpus);
            conf.setLong("q2.N", n);                                  // handed to Job 2 through the Configuration

            // ---- Job 2 : positional inverted index ---------------------------------------------------
            Job j2 = IndexJob.build(conf, corpus, stopwords, index, reducers);
            if (!j2.waitForCompletion(true)) return fail("Job 2 (index) failed");
            printIndexCounters(j2, n);
        }

        // ---- Job 3 : phrase query ----------------------------------------------------------------------
        if (phrase == null) {
            System.out.println("[Q2] no -Dphrase given: Job 3 skipped");
            return 0;
        }
        fs.delete(phraseOut, true);
        Job j3 = PhraseJob.build(conf, new Path(index, "part-r-*"), phraseOut);
        if (!j3.waitForCompletion(true)) return fail("Job 3 (phrase) failed");
        printPhraseCounters(j3, PhraseJob.phraseTerms(conf));
        return 0;
    }

    private static int fail(String msg) {
        System.err.println("[Q2] " + msg);
        return 1;
    }

    private static void line(String name, long v) {
        System.out.println("[Q2]   " + name + " = " + v);
    }

    private static void printDocCounters(Counters c) {
        for (Q2Counter k : new Q2Counter[] { Q2Counter.DOCS, Q2Counter.BLANK_LINES, Q2Counter.HEADER_SKIPPED, Q2Counter.MALFORMED_LINES }) {
            line(k.name(), c.findCounter(k).getValue());
        }
    }

    private static void printIndexCounters(Job j, long n) throws Exception {
        Counters c = j.getCounters();
        System.out.println("[Q2] ---- Job 2 (delta=" + j.getConfiguration().getBoolean("q2.delta", true)
                + ", positions=" + j.getConfiguration().get("q2.positions", "filtered") + ", N=" + n + ") ----");
        for (Q2Counter k : new Q2Counter[] {
                Q2Counter.EMPTY_BODY_DOCS, Q2Counter.EMPTY_TITLE_DOCS, Q2Counter.EXTRA_FIELDS_MERGED,
                Q2Counter.DOCS_WITHOUT_INDEXABLE_TOKENS, Q2Counter.STOPWORDS_LOADED, Q2Counter.TOKENS_TOTAL,
                Q2Counter.TOKENS_DROPPED_SHORT, Q2Counter.TOKENS_DROPPED_STOPWORD, Q2Counter.TOKENS_KEPT,
                Q2Counter.TERM_IN_TITLE_ONLY_OF_DOC, Q2Counter.TERMS_INDEXED, Q2Counter.TERMS_CORPUS_STOP,
                Q2Counter.TERMS_RARE, Q2Counter.POSTINGS_WRITTEN }) {
            line(k.name(), c.findCounter(k).getValue());
        }
        line("MAP_INPUT_RECORDS", c.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
        line("MAP_OUTPUT_RECORDS", c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
        line("MAP_OUTPUT_BYTES", c.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue());
        line("COMBINE_INPUT_RECORDS", c.findCounter(TaskCounter.COMBINE_INPUT_RECORDS).getValue());
        line("REDUCE_SHUFFLE_BYTES", c.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue());
        line("REDUCE_INPUT_GROUPS (distinct terms)", c.findCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue());
        line("REDUCE_INPUT_RECORDS", c.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue());
        line("TOTAL_LAUNCHED_MAPS", c.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS).getValue());
    }

    private static void printPhraseCounters(Job j, List<String> terms) throws Exception {
        Counters c = j.getCounters();
        System.out.println("[Q2] ---- Job 3 (phrase = \"" + String.join(" ", terms) + "\") ----");
        for (Q2Counter k : new Q2Counter[] {
                Q2Counter.PHRASE_INDEX_LINES_READ, Q2Counter.PHRASE_DOCS_WITH_ALL_WORDS, Q2Counter.PHRASE_DOCS_MATCHED,
                Q2Counter.PHRASE_DOCS_ALL_WORDS_NOT_ADJACENT, Q2Counter.PHRASE_OCCURRENCES }) {
            line(k.name(), c.findCounter(k).getValue());
        }
        for (String t : new java.util.LinkedHashSet<>(terms)) {
            Counter tc = c.getGroup("Q2_PHRASE_TERMS").findCounter(t);
            if (tc.getValue() == 0) {
                System.out.println("[Q2]   WARNING: \"" + t + "\" is not in the main index (stop-word, corpus_stop, rare or absent), "
                        + "so no document can match this phrase");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Q2Driver(), args));
    }
}
