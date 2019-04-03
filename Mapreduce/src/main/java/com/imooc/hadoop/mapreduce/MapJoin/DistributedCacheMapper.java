package com.imooc.hadoop.mapreduce.MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class DistributedCacheMapper extends Mapper<AggregatedLogFormat.LogWriter, Text,Text, NullWritable> {
    Text k = new Text();
    HashMap<String, String> pdMap = new HashMap<String, String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("pd.txt"), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            //1.切割
            String[] fileds = line.split("\t");
            pdMap.put(fileds[0],fileds[1]);
        }
        //关闭资源
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(AggregatedLogFormat.LogWriter key, Text value, Context context) throws IOException, InterruptedException {
        String[] fileds = value.toString().split("\t");
        //拼接
        String line = fileds[0]+"\t"+pdMap.get(fileds[1])+"\t"+fileds[2];

        k.set(line);

        context.write(k,NullWritable.get());
    }
}
