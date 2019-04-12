package com.imooc.hadoop.mapreduce.youtube;

import com.imooc.hadoop.mapreduce.WordCount.WordCountMapper;
import com.imooc.hadoop.mapreduce.WordCount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class YoutubeDriver  implements Tool {
    private Configuration conf = null;

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        YoutubeDriver youtubeDriver = new YoutubeDriver();
        int resultCode = ToolRunner.run(youtubeDriver,args);
        System.exit(resultCode);
    }
    @Override
    public int run(String[] args) throws Exception {
        conf.set("inpaths",args[0]);
        conf.set("outpaths",args[1]);

        Job job = Job.getInstance(conf);

        //指定本次mr job jar包运行主类
        job.setJarByClass(YoutubeDriver.class);

        //指定本次mr 所用的mapper reducer类分别是什么
        job.setMapperClass(YoutubeMapper.class);

        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //如果业务有需求，就可以设置combiner组件
        job.setCombinerClass(WordCountReducer.class);

        job.setNumReduceTasks(0);
        //指定本次mr 输入的数据路径 和最终输出结果存放在什么位置
       this.initJobIntputPath(job);
       this.initJobOutputPath(job);

        return job.waitForCompletion(true)?1:-1;
    }

    private void initJobOutputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String outPathString = conf.get("outpaths");
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outPathString);

        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);
    }

    private void initJobIntputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String inPathString = conf.get("inpaths");

        Path inPath = new Path(inPathString);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(inPath)){
            FileInputFormat.addInputPath(job,inPath);
        }else {
            throw new IOException("传输的输入路径不合法");
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
