package com.iitbh.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureReducer
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            sum += Integer.parseInt(parts[0]);
            count += Integer.parseInt(parts[1]);
        }

        double average = (double) sum / count;

        context.write(key, new Text(String.valueOf(average)));
    }
}