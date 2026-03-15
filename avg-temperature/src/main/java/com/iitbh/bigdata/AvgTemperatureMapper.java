package com.iitbh.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgTemperatureMapper extends Mapper<Object, Text, Text, Text> {

    private Text year = new Text();
    private Text tempCount = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split(",");

        String yearStr = parts[0];
        String tempStr = parts[1];

        year.set(yearStr);
        tempCount.set(tempStr + ",1");

        context.write(year, tempCount);
    }
}