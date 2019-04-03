package com.imooc.hadoop.mapreduce.OutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosBiliBili;
    FSDataOutputStream fosOther;

    public FRecordWriter(TaskAttemptContext job) {
        try {
            //获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //创建输出到bilibili.log
            fosBiliBili = fs.create(new Path("F:\\mapreduce\\outputformat\\bilibili.log"));
            //创建输出到other.log
            fosOther = fs.create(new Path("F:\\mapreduce\\outputformat\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断key当中是否有Bilibili 有输出到bilibili.log 没有输出到other.log
        if(key.toString().contains("bilibili")){
            fosBiliBili.write(key.toString().getBytes());
        }else {
            fosOther.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关闭资源
        IOUtils.closeStream(fosBiliBili);
        IOUtils.closeStream(fosOther);
    }
}
