package com.imooc.hadoop.mapreduce.SortPartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class SortRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(SortRunner.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(AreaPartitioner.class);
        job.setNumReduceTasks(6);

        FileInputFormat.setInputPaths(job, new Path("F:\\mapreduce\\sortpartition\\input"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\mapreduce\\sortpartition\\output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
