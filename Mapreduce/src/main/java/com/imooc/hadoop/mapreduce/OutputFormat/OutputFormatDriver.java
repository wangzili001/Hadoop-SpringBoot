package com.imooc.hadoop.mapreduce.OutputFormat;

import com.imooc.hadoop.mapreduce.OrderSortByGroup.Order;
import com.imooc.hadoop.mapreduce.OrderSortByGroup.OrderMapper;
import com.imooc.hadoop.mapreduce.OrderSortByGroup.OrderReducer;
import com.imooc.hadoop.mapreduce.OrderSortByGroup.OrderSortGrouping;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class OutputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"F:\\mapreduce\\outputformat\\input","F:\\mapreduce\\outputformat\\output"};
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(OutputFormatDriver.class);

        job.setMapperClass(OutputFormatMapper.class);
        job.setReducerClass(OutputFormatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //将自定义的输出格式组件设置到job中
        job.setOutputFormatClass(FilterOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
