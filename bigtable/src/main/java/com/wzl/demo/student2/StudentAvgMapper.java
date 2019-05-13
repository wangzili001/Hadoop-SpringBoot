package com.wzl.demo.student2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

public class StudentAvgMapper extends Mapper<LongWritable, Text,StudentBean, NullWritable> {
    StudentBean studentBean = new StudentBean();
    DecimalFormat df = new DecimalFormat("#.0");
    float avg = 0.0f;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        studentBean.setCourse(split[0]);
        studentBean.setName(split[1]);
        int sum = 0;
        for(int i = 2;i<split.length;i++){
            sum += Integer.parseInt(split[i]);
        }
        avg = sum/(split.length-2);
        studentBean.setAvg(Float.parseFloat(df.format(avg)));
        context.write(studentBean,NullWritable.get());
    }

}
