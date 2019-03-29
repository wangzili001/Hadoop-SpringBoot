package com.imooc.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public  class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text k2, Iterable<FlowBean> v2s,Context context) throws IOException, InterruptedException {
        long up_flow = 0;
        long down_flow = 0;
        for (FlowBean v2 : v2s) {
            up_flow += v2.getUp_flow();
            down_flow += v2.getDown_flow();
        }
        context.write(new Text(k2), new FlowBean(k2.toString(), up_flow, down_flow));
    }
}
