package ca2.q4;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/** Partitioner on user_id ONLY: every event of a user reaches the same reducer, whatever its timestamp. */
public class UserPartitioner extends Partitioner<UserTsKey, NullWritable> {
    @Override
    public int getPartition(UserTsKey key, NullWritable value, int numPartitions) {
        return (key.getUser().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
