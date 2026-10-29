package ca2.q5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** count and three INR totals (E4): order amount, net paid, settled. */
public class SummaryWritable implements Writable {
    public long count, orderE4, netE4, settledE4;

    public void set(long c, long o, long n, long s) { count = c; orderE4 = o; netE4 = n; settledE4 = s; }

    public void add(SummaryWritable w) { count += w.count; orderE4 += w.orderE4; netE4 += w.netE4; settledE4 += w.settledE4; }

    public void clear() { count = orderE4 = netE4 = settledE4 = 0; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count); out.writeLong(orderE4); out.writeLong(netE4); out.writeLong(settledE4);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong(); orderE4 = in.readLong(); netE4 = in.readLong(); settledE4 = in.readLong();
    }
}
