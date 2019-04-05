package com.imooc.hadoop.mapreduce.Jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JobsMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    String name;
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");

        for (String split : splits) {
            k.set(split+"--"+name);
            context.write(k,v);
        }
    }
}
