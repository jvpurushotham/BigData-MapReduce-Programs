package ca2.q4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** SortComparator: (user_id, event_ts) - and, only for exact ties, url (see UserTsKey). */
public class UserTsSortComparator extends WritableComparator {
    public UserTsSortComparator() {
        super(UserTsKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((UserTsKey) a).compareTo((UserTsKey) b);
    }
}
