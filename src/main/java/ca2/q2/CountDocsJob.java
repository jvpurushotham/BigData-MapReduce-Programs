package ca2.q2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Job 1 (question 2b): count the documents.  Map-only, no output files: the answer is the custom counter
 * Q2Counter.DOCS, which the driver reads and hands to Job 2 through the job Configuration (key q2.N).
 */
public class CountDocsJob {

    public static class CountMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
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
            ctx.getCounter(Q2Counter.DOCS).increment(1);
        }
    }

    public static Job build(Configuration conf, Path corpus) throws IOException {
        Job job = Job.getInstance(conf, "q2-1-count-docs");
        job.setJarByClass(CountDocsJob.class);
        job.setMapperClass(CountMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileInputFormat.addInputPath(job, corpus);
        return job;
    }
}
