package com.imooc.hadoop.mapreduce.TableJoin;

import com.imooc.hadoop.mapreduce.SortPartition.AreaPartitioner;
import com.imooc.hadoop.mapreduce.SortPartition.FlowBean;
import com.imooc.hadoop.mapreduce.SortPartition.SortMapper;
import com.imooc.hadoop.mapreduce.SortPartition.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TableRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        args = new String[]{"F:\\mapreduce\\TableOrder\\input","F:\\mapreduce\\TableOrder\\output"};
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(TableRunner.class);

        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
