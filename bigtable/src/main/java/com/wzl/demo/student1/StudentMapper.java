package com.wzl.demo.student1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StudentMapper extends Mapper<LongWritable, Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(",");
        k.set(splits[0]);
        int totalTims = splits.length -2;
        int sum = 0;
        for(int i=2;i<splits.length;i++){
            sum+=Integer.parseInt(splits[i]);
        }
        v.set(totalTims+"_"+sum);
        context.write(k,v);
    }
}
