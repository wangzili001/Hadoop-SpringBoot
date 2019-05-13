package com.wzl.demo.student1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StudentReducer extends Reducer<Text,Text,Text,Text> {
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int sumGrade = 0;
        for (Text value : values) {
            String[] split = value.toString().split("_");
            sum += Integer.parseInt(split[0]);
            sumGrade += Integer.parseInt(split[1]);
        }
        float avgGrade = sumGrade/sum;
        v.set(sum+"\t"+avgGrade);
        context.write(key,v);
    }
}
