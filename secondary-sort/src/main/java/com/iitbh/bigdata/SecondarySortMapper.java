package com.iitbh.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper
        extends Mapper<LongWritable, Text, CompositeKey, Text> {

    private CompositeKey compositeKey = new CompositeKey();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split(",");

        int userId = Integer.parseInt(parts[0]);
        String date = parts[1];
        String amount = parts[2];

        compositeKey.set(userId, date);

        context.write(compositeKey, new Text(date + "," + amount));
    }
}