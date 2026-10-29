package ca2.q1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** (total readings, outlier readings) for one meter-month; summed by the Job C reducer. */
public class CountPairWritable implements Writable {
    public long total;
    public long outliers;

    public void set(long total, long outliers) {
        this.total = total;
        this.outliers = outliers;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, total);
        WritableUtils.writeVLong(out, outliers);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        total = WritableUtils.readVLong(in);
        outliers = WritableUtils.readVLong(in);
    }
}
