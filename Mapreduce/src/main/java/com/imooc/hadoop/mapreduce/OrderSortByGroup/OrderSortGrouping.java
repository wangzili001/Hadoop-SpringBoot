package com.imooc.hadoop.mapreduce.OrderSortByGroup;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderSortGrouping extends WritableComparator {

    protected OrderSortGrouping(){
        super(Order.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Order aBean = (Order) a;
        Order bBean = (Order) b;
        int result;
        if(aBean.getId()>bBean.getId()){
            result = 1;
        }else if(aBean.getId()<bBean.getId()){
            result = -1;
        }else {
            result = 0;
        }
        return result;
    }
}
