package com.iitbh.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class UsersMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text userId = new Text();
    private Text record = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split(",");

        if (parts.length == 3) {
            userId.set(parts[0]);
            record.set("USER," + parts[1] + "," + parts[2]);
            context.write(userId, record);
        }
    }
}