package com.imooc.hadoop.mapreduce.Partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSumArea {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumArea.class);

        job.setMapperClass(FlowSumAreaMapper.class);
        job.setReducerClass(FlowSumAreaReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 定义分组逻辑类
        job.setPartitionerClass(AreaPartitioner.class);
        // 设定reducer的任务并发数,应该跟分组的数量保持一致
        job.setNumReduceTasks(6);

        FileInputFormat.setInputPaths(job, new Path("F:\\mapreduce\\Partitioner\\input"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\mapreduce\\Partitioner\\output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
