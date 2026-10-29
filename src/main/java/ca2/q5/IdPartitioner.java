package ca2.q5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** Partitioner on the join id ONLY (not on the tag): the "one" and the "many" records of an id reach the same reducer. */
public class IdPartitioner extends Partitioner<TaggedKey, Text> {
    @Override
    public int getPartition(TaggedKey key, Text value, int numPartitions) {
        return (key.getId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
