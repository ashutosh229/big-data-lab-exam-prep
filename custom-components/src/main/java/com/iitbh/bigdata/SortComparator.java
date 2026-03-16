package com.iitbh.bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

    protected SortComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CompositeKey k1 = (CompositeKey) a;
        CompositeKey k2 = (CompositeKey) b;

        int result = k1.getUserId().compareTo(k2.getUserId());

        if(result == 0)
            result = -k1.getTimestamp().compareTo(k2.getTimestamp());

        return result;
    }
}