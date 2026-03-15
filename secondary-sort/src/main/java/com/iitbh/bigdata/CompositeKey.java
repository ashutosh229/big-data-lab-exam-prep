package com.iitbh.bigdata;

import java.io.*;
import org.apache.hadoop.io.*;

public class CompositeKey implements WritableComparable<CompositeKey> {

    private IntWritable userId;
    private Text date;

    public CompositeKey() {
        this.userId = new IntWritable();
        this.date = new Text();
    }

    public void set(int userId, String date) {
        this.userId.set(userId);
        this.date.set(date);
    }

    public IntWritable getUserId() {
        return userId;
    }

    public Text getDate() {
        return date;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        userId.write(out);
        date.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId.readFields(in);
        date.readFields(in);
    }

    @Override
    public int compareTo(CompositeKey o) {

        int result = userId.compareTo(o.userId);

        if (result == 0) {
            result = date.compareTo(o.date);
        }

        return result;
    }
}