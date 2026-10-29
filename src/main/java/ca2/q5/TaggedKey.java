package ca2.q5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Tagged composite key of a reduce-side join: (join id, tag). tag 0 = the "ONE" side (order / transaction),
 * tag 1 = the "MANY" side (transactions / settlements). Sorted by id and then tag, so inside one id the single "one"
 * record always arrives BEFORE the stream of "many" records.
 */
public class TaggedKey implements WritableComparable<TaggedKey> {
    private String id = "";
    private int tag;

    public TaggedKey() { }

    public void set(String id, int tag) { this.id = id; this.tag = tag; }
    public String getId() { return id; }
    public int getTag() { return tag; }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, id);
        out.writeByte(tag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = Text.readString(in);
        tag = in.readByte();
    }

    @Override
    public int compareTo(TaggedKey o) {
        int c = id.compareTo(o.id);
        return c != 0 ? c : Integer.compare(tag, o.tag);
    }

    @Override
    public boolean equals(Object o) { return o instanceof TaggedKey && compareTo((TaggedKey) o) == 0; }

    @Override
    public int hashCode() { return id.hashCode(); }

    @Override
    public String toString() { return id + "#" + tag; }
}
