package ca2.q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** (net units, net revenue in E4 units): both are plain sums, so merging is associative and a Combiner is safe. */
public class NetWritable implements Writable {
    public long units;
    public long revenueE4;

    public void set(long units, long revenueE4) {
        this.units = units;
        this.revenueE4 = revenueE4;
    }

    public void merge(NetWritable o) {
        units += o.units;
        revenueE4 += o.revenueE4;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, units);
        WritableUtils.writeVLong(out, revenueE4);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        units = WritableUtils.readVLong(in);
        revenueE4 = WritableUtils.readVLong(in);
    }
}
