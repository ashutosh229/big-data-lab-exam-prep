package com.iitbh.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class CustomDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Custom Example");

        job.setJarByClass(CustomDriver.class);

        job.setPartitionerClass(CustomPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setSortComparatorClass(SortComparator.class);

    }
}