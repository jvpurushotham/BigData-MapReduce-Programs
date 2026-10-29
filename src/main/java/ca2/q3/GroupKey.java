package ca2.q3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/** (category, month): the Top-N group. Sorted by category, then month. */
public class GroupKey implements WritableComparable<GroupKey> {
    public String category = "";
    public String month = "";

    public GroupKey() { }

    public void set(String category, String month) {
        this.category = category;
        this.month = month;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, category);
        Text.writeString(out, month);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        category = Text.readString(in);
        month = Text.readString(in);
    }

    @Override
    public int compareTo(GroupKey o) {
        int c = category.compareTo(o.category);
        return c != 0 ? c : month.compareTo(o.month);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof GroupKey && compareTo((GroupKey) o) == 0;
    }

    @Override
    public int hashCode() {
        return category.hashCode() * 31 + month.hashCode();
    }

    @Override
    public String toString() {
        return category + "\t" + month;
    }
}
