package com.imooc.hadoop.mapreduce.Jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoJobsMapper extends Mapper<LongWritable,Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        String[] split = splits[0].split("--");
        k.set(split[0]);
        v.set(split[1]+"-->"+splits[1]);
        context.write(k,v);
    }
}
