package com.imooc.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class AreaPartitioner extends Partitioner<Text, FlowBean> {

    private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();

    static {
        areaMap.put("135", 0);
        areaMap.put("136", 1);
        areaMap.put("137", 2);
        areaMap.put("138", 3);
        areaMap.put("139", 4);
    }

    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        // 从key中拿到手机号，查询手机归属地字典，不同的省份返回不同的组号
        Integer areCoder = areaMap.get(text.toString().substring(0, 3));
        if (areCoder == null) {
            areCoder = 5;
        }
        return areCoder;
    }
}
