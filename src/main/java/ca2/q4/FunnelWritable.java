package ca2.q4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** Five sums: [0] sessions, [1] reached /home, [2] reached /product/*, [3] reached /cart, [4] reached /checkout/success. */
public class FunnelWritable implements Writable {
    public final long[] c = new long[5];

    public FunnelWritable() { }

    /** One session whose deepest valid stage is {@code stage} (0..4). */
    public void setSession(int stage) {
        c[0] = 1;
        for (int i = 1; i <= 4; i++) c[i] = (stage >= i) ? 1 : 0;
    }

    public void add(FunnelWritable o) {
        for (int i = 0; i < 5; i++) c[i] += o.c[i];
    }

    public void clear() {
        for (int i = 0; i < 5; i++) c[i] = 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (long v : c) out.writeLong(v);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < 5; i++) c[i] = in.readLong();
    }
}
