package ca2.q1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Partial aggregate (count, sum, sumOfSquares, min, max) of kWh readings.
 *
 * Values are stored as integer micro-kWh (kWh * 1,000,000) so that sum and sumOfSquares are exact
 * longs. Integer addition is associative and commutative, therefore merging partial results in any
 * order (with or without a Combiner) gives bit-identical totals.
 */
public class StatsWritable implements Writable {
    public long count;
    public long sum;
    public long sumSq;
    public long min = Long.MAX_VALUE;
    public long max = Long.MIN_VALUE;

    public StatsWritable() { }

    public void setSingle(long microKwh) {
        count = 1;
        sum = microKwh;
        sumSq = microKwh * microKwh;
        min = microKwh;
        max = microKwh;
    }

    public void clear() {
        count = 0;
        sum = 0;
        sumSq = 0;
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
    }

    /** Associative + commutative merge: this is why a Combiner is safe for these statistics. */
    public void merge(StatsWritable o) {
        count += o.count;
        sum += o.sum;
        sumSq += o.sumSq;
        if (o.min < min) min = o.min;
        if (o.max > max) max = o.max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, count);
        WritableUtils.writeVLong(out, sum);
        WritableUtils.writeVLong(out, sumSq);
        WritableUtils.writeVLong(out, min);
        WritableUtils.writeVLong(out, max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = WritableUtils.readVLong(in);
        sum = WritableUtils.readVLong(in);
        sumSq = WritableUtils.readVLong(in);
        min = WritableUtils.readVLong(in);
        max = WritableUtils.readVLong(in);
    }

    @Override
    public String toString() {
        return count + "," + sum + "," + sumSq + "," + min + "," + max;
    }
}
