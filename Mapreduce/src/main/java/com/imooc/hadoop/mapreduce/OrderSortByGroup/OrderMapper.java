package com.imooc.hadoop.mapreduce.OrderSortByGroup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text,Order, NullWritable> {
    Order k = new Order();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] price = value.toString().split("\t");
        k.setId(Integer.parseInt(price[0]));
        k.setPrice(Integer.parseInt(price[2]));
        context.write(k,NullWritable.get());
    }
}
