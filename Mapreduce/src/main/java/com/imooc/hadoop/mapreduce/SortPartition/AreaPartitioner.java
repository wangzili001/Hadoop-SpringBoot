package com.imooc.hadoop.mapreduce.SortPartition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.util.HashMap;

public class AreaPartitioner extends HashPartitioner<FlowBean, NullWritable> {
    private static HashMap<String,Integer> areaMap = new HashMap();
    static {
        areaMap.put("135", 0);
        areaMap.put("137", 2);
        areaMap.put("136", 1);
        areaMap.put("138", 3);
        areaMap.put("139", 4);
    }

    @Override
    public int getPartition(FlowBean key, NullWritable value, int numReduceTasks) {
        String phone = key.getPhoneNB().substring(0, 3);
        return areaMap.get(phone)==null?5:areaMap.get(phone);
    }
}
