package com.wzl.demo.student2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StudentAvgReducer extends Reducer<StudentBean, NullWritable,StudentBean,NullWritable> {
    @Override
    protected void reduce(StudentBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //因为自定义了分区组件，自定义类型有排序规则，所以这里直接输出就可以了
        for (NullWritable nullWritable : values) {
            context.write(key, nullWritable);
        }
    }
}
