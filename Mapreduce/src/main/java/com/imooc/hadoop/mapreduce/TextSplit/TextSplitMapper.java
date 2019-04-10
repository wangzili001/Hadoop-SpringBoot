package com.imooc.hadoop.mapreduce.TextSplit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class TextSplitMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text k = new Text();
    HashMap<String, Integer> sourceMap = new HashMap<String, Integer>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            //1.切割
            sourceMap.put(line,1);
        }
        //关闭资源
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        if(null != sourceMap.get(s)){
            context.write(new Text(s), NullWritable.get());
        }
    }
}
