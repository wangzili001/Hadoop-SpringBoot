package com.imooc.hadoop.mapreduce.KeyValueTextInputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入数据  ：
 *  banzhang ni hao
 *  xihuan hadoop banzhang
 *  banzhang ni hao
 *  xihuan hadoop banzhang
 *  期望结果
 *  banzhang 2
 *  xihuan 2
 */
public class KeyValueMapper extends Mapper<Text, Text, Text, IntWritable> {
    IntWritable v = new IntWritable(1);
    //传入数据为banzhang ni hao   key为banzhang
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //1.封装数据
        //2.写出数据
        context.write(key,v);
    }
}
