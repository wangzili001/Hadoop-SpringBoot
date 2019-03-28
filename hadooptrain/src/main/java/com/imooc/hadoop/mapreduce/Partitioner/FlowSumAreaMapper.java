package com.imooc.hadoop.mapreduce.Partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String line = v1.toString();
        String[] fields = StringUtils.split(line, "\t");

        String phoneNB = fields[1];
        Long up_flow = Long.parseLong(fields[7]);
        Long down_flow = Long.parseLong(fields[8]);

        context.write(new Text(phoneNB), new FlowBean(phoneNB, up_flow, down_flow));
    }
}
