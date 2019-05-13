package com.wzl.demo.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class WordCountRunner implements Tool {
    private Configuration conf;
    private FileSystem fs;

    public int run(String[] args) throws Exception {
        conf.set("inputPath",args[0]);
        conf.set("outputPath",args[1]);

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountRunner.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(0);

        //指定本次mr 输入的数据路径 和最终输出结果存放在什么位置
        this.initJobIntputPath(job);
        this.initJobOutputPath(job);

        return job.waitForCompletion(true)?1:-1;
    }

    private void initJobOutputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String outputPath = conf.get("outputPath");

        Path path = new Path(outputPath);
        fs = FileSystem.get(conf);
        if(fs.exists(path)){
            fs.delete(path,true);
            FileOutputFormat.setOutputPath(job,path);
        }
    }

    private void initJobIntputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String inputPath = conf.get("inputPath");
        Path path = new Path(inputPath);
        fs = FileSystem.get(conf);
        if(fs.exists(path)){
            FileInputFormat.addInputPath(job,path);
        }else {
            throw  new IOException("传入路径不合法");
        }
    }

    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        if(args.length<2){
            throw new Exception("传入参数有误");
        }
        BasicConfigurator.configure();
        WordCountRunner wordCountRunner = new WordCountRunner();
        int succ = ToolRunner.run(wordCountRunner, args);
        System.exit(succ);
    }
}
