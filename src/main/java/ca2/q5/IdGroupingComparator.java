package ca2.q5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** GroupingComparator on the id only: ONE reduce() call per id; the tagged records arrive in tag order (one side first). */
public class IdGroupingComparator extends WritableComparator {
    public IdGroupingComparator() { super(TaggedKey.class, true); }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) { return ((TaggedKey) a).getId().compareTo(((TaggedKey) b).getId()); }
}
