package ca2.q4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** GroupingComparator on user_id only: ONE reduce() call per user; its values arrive in (event_ts) order. */
public class UserGroupingComparator extends WritableComparator {
    public UserGroupingComparator() {
        super(UserTsKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((UserTsKey) a).getUser().compareTo(((UserTsKey) b).getUser());
    }
}
