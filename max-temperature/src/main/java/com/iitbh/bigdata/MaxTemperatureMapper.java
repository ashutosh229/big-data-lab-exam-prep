package com.iitbh.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper 
    extends Mapper<Object, Text, Text, IntWritable> {

    private Text year = new Text();
    private IntWritable temperature = new IntWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split(",");

        String yearStr = parts[0];
        int temp = Integer.parseInt(parts[1]);

        year.set(yearStr);
        temperature.set(temp);

        context.write(year, temperature);
    }
}