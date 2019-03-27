package com.imooc.hadoop.mapreduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        // 即使没有下面这行,也可以本地运行 因\hadoop-mapreduce-client-core-2.7.4.jar!\mapred-default.xml 中默认的参数就是 local
        //conf.set("mapreduce.framework.name","local");
        Job job = Job.getInstance(conf);

        //指定本次mr job jar包运行主类
        job.setJarByClass(WordCountDriver.class);

        //指定本次mr 所用的mapper reducer类分别是什么
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定本次mr mapper阶段的输出  k  v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // job.setNumReduceTasks(3); //ReduceTask个数

        //如果业务有需求，就可以设置combiner组件
        job.setCombinerClass(WordCountReducer.class);

        //指定本次mr 输入的数据路径 和最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"F:\\wordcount\\input");
        FileOutputFormat.setOutputPath(job,new Path("F:\\wordcount\\output"));
        //如果出现0644错误或找不到winutils.exe,则需要设置windows环境和相关文件.

        //上面的路径是本地测试时使用，如果要打包jar到hdfs上运行时，需要使用下面的路径。
        //FileInputFormat.setInputPaths(job,"/wordcount/input");
        //FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));

        // job.submit(); //一般不要这个.
        //提交程序  并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
