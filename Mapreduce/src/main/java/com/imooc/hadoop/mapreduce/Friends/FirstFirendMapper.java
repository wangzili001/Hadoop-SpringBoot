package com.imooc.hadoop.mapreduce.Friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FirstFirendMapper extends Mapper<LongWritable,Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        String user = split[0];
        String[] friends = split[1].split(",");
        for (String friend : friends) {
            k.set(friend);
            v.set(user);
            context.write(k,v);
        }
    }
}
