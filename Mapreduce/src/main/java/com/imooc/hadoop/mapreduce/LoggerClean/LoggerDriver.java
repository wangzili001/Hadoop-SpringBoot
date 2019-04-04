package com.imooc.hadoop.mapreduce.LoggerClean;

import com.imooc.hadoop.mapreduce.MapJoin.DistributedCacheMapper;
import com.imooc.hadoop.mapreduce.MapJoin.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class LoggerDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        args = new String[]{"F:\\mapreduce\\logger\\input","F:\\mapreduce\\logger\\output"};
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(LoggerDriver.class);

        job.setMapperClass(LoggerMapper.class);
        //没有reduce阶段
//        job.setReducerClass(TableReducer.class);
        //map输出为最终输出
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //数据清洗不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
