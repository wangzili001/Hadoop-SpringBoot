package com.imooc.hadoop.mapreduce.PhonePayLoad;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//https://blog.csdn.net/xw_classmate/article/details/50639848#
public class FlowSumMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 一行数据
        String line = value.toString();
        // 切分数据
        String[] fields = StringUtils.split(line, "\t");
        // 得到想要的手机号、上行流量、下行流量
        String phoneNB = fields[1];
        long up_flow = Long.parseLong(fields[7]);
        long down_flow = Long.parseLong(fields[8]);
        // 封装数据为kv并输出
        context.write(new Text(phoneNB), new FlowBean(phoneNB, up_flow, down_flow));
    }
}
