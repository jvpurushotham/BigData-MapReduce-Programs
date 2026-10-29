package ca2.q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * One product inside a Top-N group: (product_id, net revenue E4, net units).
 *
 * RANK ORDER is a STRICT TOTAL ORDER:  revenue descending, then units descending, then product_id ascending.
 * Two entries compare equal only if all three fields are equal, and product_id is unique inside a group, so no two
 * different products can ever compare equal. A TreeSet using this comparator therefore never silently drops a product
 * with the same revenue as another one (the classic TreeMap<revenue, product> key-collision bug).
 */
public class EntryWritable implements Writable {

    public static final Comparator<EntryWritable> RANK = (a, b) -> {
        if (a.revenueE4 != b.revenueE4) return a.revenueE4 > b.revenueE4 ? -1 : 1;
        if (a.units != b.units) return a.units > b.units ? -1 : 1;
        return a.productId.compareTo(b.productId);
    };

    public String productId = "";
    public long revenueE4;
    public long units;

    public EntryWritable() { }

    public EntryWritable(String productId, long revenueE4, long units) {
        this.productId = productId;
        this.revenueE4 = revenueE4;
        this.units = units;
    }

    public EntryWritable copy() {
        return new EntryWritable(productId, revenueE4, units);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, productId);
        WritableUtils.writeVLong(out, revenueE4);
        WritableUtils.writeVLong(out, units);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        productId = Text.readString(in);
        revenueE4 = WritableUtils.readVLong(in);
        units = WritableUtils.readVLong(in);
    }
}
