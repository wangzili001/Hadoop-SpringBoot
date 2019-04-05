package com.imooc.hadoop.mapreduce.Friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoFirendReducer extends Reducer<Text,Text,Text,Text> {
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value+" ");
        }
        sb.deleteCharAt(sb.length()-1);
        v.set(sb.toString());
        context.write(key,v);
    }
}
