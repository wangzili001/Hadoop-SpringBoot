package com.wzl.demo.student1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class StudentRunner implements Tool {
    private Configuration conf;
    private FileSystem fs;

    public int run(String[] args) throws Exception {
        conf.set("inputPath",args[0]);
        conf.set("outputPath",args[1]);

        Job job = Job.getInstance(conf);

        job.setJarByClass(StudentRunner.class);

        job.setMapperClass(StudentMapper.class);
        job.setReducerClass(StudentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setNumReduceTasks(4);

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
        StudentRunner studentRunner = new StudentRunner();
        int succ = ToolRunner.run(studentRunner, args);
        System.exit(succ);
    }
}
