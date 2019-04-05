package com.imooc.hadoop.mapreduce.Jobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoJobsReducer extends Reducer<Text,Text,Text,Text> {
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer result = new StringBuffer();
        for (Text value : values) {
            result.append(value.toString()+" ");
        }
        v.set(String.valueOf(result));
        context.write(key,v);
    }
}
