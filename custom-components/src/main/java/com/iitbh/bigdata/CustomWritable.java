package com.iitbh.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CustomWritable implements Writable {

    private Text field1;
    private Text field2;

    public CustomWritable() {
        this.field1 = new Text();
        this.field2 = new Text();
    }

    public CustomWritable(String f1, String f2) {
        this.field1 = new Text(f1);
        this.field2 = new Text(f2);
    }

    public Text getField1() { return field1; }
    public Text getField2() { return field2; }

    @Override
    public void write(DataOutput out) throws IOException {
        field1.write(out);
        field2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        field1.readFields(in);
        field2.readFields(in);
    }

    @Override
    public String toString() {
        return field1 + "," + field2;
    }
}