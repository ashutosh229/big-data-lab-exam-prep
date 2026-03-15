package com.iitbh.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideJoinReducer
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String userInfo = "";
        List<String> logs = new ArrayList<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");

            if (parts[0].equals("USER")) {
                userInfo = parts[1] + "," + parts[2];
            } else if (parts[0].equals("LOG")) {
                logs.add(parts[1] + "," + parts[2]);
            }
        }

        for (String log : logs) {
            context.write(key, new Text(userInfo + "," + log));
        }
    }
}