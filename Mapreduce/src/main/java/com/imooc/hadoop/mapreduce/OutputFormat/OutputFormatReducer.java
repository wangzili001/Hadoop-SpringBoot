package com.imooc.hadoop.mapreduce.OutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OutputFormatReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //防止重复数据
        for (NullWritable value : values) {
            k.set(key.toString()+"\r\n");
            context.write(k,NullWritable.get());
        }
    }
}
