package com.iitbh.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer
        extends Reducer<CompositeKey, Text, IntWritable, Text> {

    public void reduce(CompositeKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text val : values) {
            context.write(key.getUserId(), val);
        }
    }
}