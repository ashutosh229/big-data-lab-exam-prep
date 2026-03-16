package com.iitbh.bigdata;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class CustomPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {

        return Math.abs(key.hashCode()) % numPartitions;
    }
}