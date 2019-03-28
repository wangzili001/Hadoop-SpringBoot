package com.imooc.hadoop.mapreduce.PhonePayLoad;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    // 框架每传递一组数据<1387788654,{flowbean,flowbean,flowbean,flowbean.....}>调用一次我们的reduce方法
    // reduce中的业务逻辑就是遍历values，然后进行累加求和再输出
    @Override
    protected void reduce(Text k2, Iterable<FlowBean> v2s, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long up_flow = 0;
        long down_flow = 0;

        for (FlowBean v2 : v2s) {
            up_flow += v2.getUp_flow();
            down_flow += v2.getDown_flow();
        }

        context.write(k2, new FlowBean(k2.toString(), up_flow, down_flow));
    }
}





