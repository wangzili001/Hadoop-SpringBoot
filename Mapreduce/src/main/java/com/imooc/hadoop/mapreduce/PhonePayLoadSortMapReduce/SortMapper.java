package com.imooc.hadoop.mapreduce.PhonePayLoadSortMapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,FlowBean,NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        String phoneNB = splits[0];
        long up_flow = Long.parseLong(splits[2]);
        long down_flow = Long.parseLong(splits[3]);
        context.write(new FlowBean(phoneNB, up_flow, down_flow), NullWritable.get());
    }
}
