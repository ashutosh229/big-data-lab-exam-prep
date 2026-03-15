package com.iitbh.bigdata;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class UserPartitioner extends Partitioner<CompositeKey, Text> {

    @Override
    public int getPartition(CompositeKey key, Text value, int numPartitions) {

        return Math.abs(key.getUserId().hashCode()) % numPartitions;
    }
}