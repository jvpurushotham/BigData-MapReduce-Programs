package ca2.q5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** SortComparator: (id, tag). */
public class TagSortComparator extends WritableComparator {
    public TagSortComparator() { super(TaggedKey.class, true); }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) { return ((TaggedKey) a).compareTo((TaggedKey) b); }
}
