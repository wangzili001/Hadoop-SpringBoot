package com.imooc.hadoop.mapreduce.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//map阶段

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    /**
     *  这里就是mapper阶段具体的业务逻辑实现方法  该方法的调用取决于读取数据的组件有没有给mr传入数据
     *  如果有的话  每传入一个《k,v》对  该方法就会被调用一次
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //拿到传入进来的一行内容，把数据类型转化为String
        String line = value.toString();

        //将这一行内容按照分隔符进行一行内容的切割 切割成一个单词数组
        String[] words = line.split(" ");

        //遍历数组，每出现一个单词  就标记一个数字1  <单词，1>
        for (String word : words) {
            //使用mr程序的上下文context 把mapper阶段处理的数据发送出去
            //作为reduce节点的输入数据
            context.write(new Text(word),new IntWritable(1));
            //hadoop hadoop spark -->   <hadoop,1><hadoop,1><spark,1>
        }
    }
}
