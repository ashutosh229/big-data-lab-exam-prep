package com.iitbh.bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

    protected GroupComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CompositeKey k1 = (CompositeKey) a;
        CompositeKey k2 = (CompositeKey) b;

        return k1.getUserId().compareTo(k2.getUserId());
    }
}