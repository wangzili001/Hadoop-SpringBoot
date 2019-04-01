package com.imooc.hadoop.mapreduce.InputFormat;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WhoFileInputMapper extends Mapper<Text, ByteWritable,Text,ByteWritable> {
    @Override
    protected void map(Text key, ByteWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
