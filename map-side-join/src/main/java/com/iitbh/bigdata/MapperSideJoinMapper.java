package com.iitbh.bigdata;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, String> userMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader("users.txt"));
        String line;

        while ((line = reader.readLine()) != null) {

            String[] parts = line.split(",");

            if (parts.length == 3) {
                userMap.put(parts[0], parts[1] + "," + parts[2]);
            }
        }

        reader.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split(",");

        if (parts.length == 3) {

            String userId = parts[0];
            String logInfo = parts[1] + "," + parts[2];

            String userInfo = userMap.get(userId);

            if (userInfo != null) {
                context.write(new Text(userId),
                        new Text(userInfo + "," + logInfo));
            }
        }
    }
}