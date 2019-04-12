package com.imooc.hadoop.mapreduce.youtube;

import com.imooc.hadoop.mapreduce.youtube.utils.FormatUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class YoutubeMapper extends Mapper<LongWritable,Text,Text, NullWritable> {
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String etlString = FormatUtils.FormatData(value.toString());
        if (etlString != null){
            k.set(etlString);
            context.write(k,NullWritable.get());
        }
    }
}
