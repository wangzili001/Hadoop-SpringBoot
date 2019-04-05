package com.imooc.hadoop.mapreduce.Friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TwoFirendMapper extends Mapper<LongWritable,Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String friend = split[0];
        v.set(friend);
        String[] user = split[1].split(",");
        Arrays.sort(user); //排序
        for(int i=0;i<user.length;i++){
            for (int j=i+1;j<user.length;j++){
                k.set(user[i].toString()+"-"+user[j].toString());
                context.write(k,v);
            }
        }
    }
}
