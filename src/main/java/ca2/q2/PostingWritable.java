package ca2.q2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * One posting: (doc_id, tf, positions[]).  This is the value that travels through the shuffle.
 *
 * Wire format
 *   doc_id      : Text.writeString   (vint length + UTF-8 bytes)
 *   tf          : WritableUtils.writeVInt
 *   n           : WritableUtils.writeVInt                 number of positions
 *   positions   : n x WritableUtils.writeVInt, DELTA (gap) encoded when q2.delta=true
 *                 [3, 10, 25]  is stored as  [3, 7, 15]   (first value is the gap from 0)
 *
 * Positions are strictly increasing, so every gap is >= 1 (>= 0 for the first one) and stays small:
 * a VInt needs 1 byte for values -112..127 but 2+ bytes for larger ones, which is where the saving comes from.
 *
 * The encoding mode is a static switch set from the job Configuration in setup() of every task that
 * serialises or de-serialises postings (mapper and reducer of Job 2); each task runs in its own JVM.
 */
public class PostingWritable implements Writable {

    private static boolean delta = true;

    public static void configure(Configuration conf) {
        delta = conf.getBoolean("q2.delta", true);
    }

    public String docId = "";
    public int tf;
    public int[] positions = new int[8];
    public int n;

    public PostingWritable() { }

    public void set(String docId, int tf, int[] src, int count) {
        this.docId = docId;
        this.tf = tf;
        if (positions.length < count) positions = new int[Math.max(count, positions.length * 2)];
        System.arraycopy(src, 0, positions, 0, count);
        this.n = count;
    }

    /** Hadoop re-uses Writable instances between calls, so the reducer must take a private copy. */
    public PostingWritable copy() {
        PostingWritable p = new PostingWritable();
        p.docId = docId;
        p.tf = tf;
        p.positions = Arrays.copyOf(positions, n);
        p.n = n;
        return p;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, docId);
        WritableUtils.writeVInt(out, tf);
        WritableUtils.writeVInt(out, n);
        int prev = 0;
        for (int i = 0; i < n; i++) {
            int v = positions[i];
            WritableUtils.writeVInt(out, delta ? v - prev : v);
            prev = v;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        docId = Text.readString(in);
        tf = WritableUtils.readVInt(in);
        n = WritableUtils.readVInt(in);
        if (positions.length < n) positions = new int[Math.max(n, positions.length * 2)];
        int prev = 0;
        for (int i = 0; i < n; i++) {
            int v = WritableUtils.readVInt(in);
            if (delta) v += prev;
            positions[i] = v;
            prev = v;
        }
    }
}
