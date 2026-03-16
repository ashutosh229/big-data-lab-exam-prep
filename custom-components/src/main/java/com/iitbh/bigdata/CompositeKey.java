package com.iitbh.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {

    private IntWritable userId;
    private Text timestamp;

    public CompositeKey() {
        userId = new IntWritable();
        timestamp = new Text();
    }

    public void set(int userId, String timestamp) {
        this.userId.set(userId);
        this.timestamp.set(timestamp);
    }

    public IntWritable getUserId() {
        return userId;
    }

    public Text getTimestamp() {
        return timestamp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        userId.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId.readFields(in);
        timestamp.readFields(in);
    }

    @Override
    public int compareTo(CompositeKey o) {

        int result = userId.compareTo(o.userId);

        if(result == 0)
            result = timestamp.compareTo(o.timestamp);

        return result;
    }

    @Override
    public String toString() {
        return userId + "," + timestamp;
    }
}