package ca2.q4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key of the secondary sort: (user_id, event_ts).
 * <p>
 * {@code url} is a TIE-BREAKER only: two events of one user with the same millisecond would otherwise reach the reducer
 * in a random order (the shuffle is not stable), which would make entry_url / exit_url and the funnel non-deterministic.
 * It takes part in the sort order (after user_id and event_ts) but NOT in partitioning or grouping.
 */
public class UserTsKey implements WritableComparable<UserTsKey> {
    private String user = "";
    private long ts;
    private String url = "";

    public UserTsKey() { }

    public void set(String user, long ts, String url) {
        this.user = user;
        this.ts = ts;
        this.url = url;
    }

    public String getUser() { return user; }
    public long getTs() { return ts; }
    public String getUrl() { return url; }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, user);
        out.writeLong(ts);
        Text.writeString(out, url);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user = Text.readString(in);
        ts = in.readLong();
        url = Text.readString(in);
    }

    /** (user_id, event_ts, url) - the full sort order. */
    @Override
    public int compareTo(UserTsKey o) {
        int c = user.compareTo(o.user);
        if (c != 0) return c;
        c = Long.compare(ts, o.ts);
        if (c != 0) return c;
        return url.compareTo(o.url);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof UserTsKey && compareTo((UserTsKey) o) == 0;
    }

    @Override
    public int hashCode() {
        return user.hashCode();           // consistent with the partitioner: user_id only
    }

    @Override
    public String toString() {
        return user + "\t" + ts + "\t" + url;
    }
}
