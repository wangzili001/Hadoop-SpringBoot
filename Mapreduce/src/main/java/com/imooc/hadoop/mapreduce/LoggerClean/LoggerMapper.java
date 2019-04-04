package com.imooc.hadoop.mapreduce.LoggerClean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LoggerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //解析数据
        String[] split = line.split(" ");
        boolean result = parseLog(line,context,split);
        if(!result){
            return;
        }

        k.set(split[0]+"\t"+split[8]+"\t"+split[10]);
        //解析通过写出去
        context.write(k,NullWritable.get());
    }

    private boolean parseLog(String line, Context context,String[] split) {
        if("200".equals(split[8])){
            context.getCounter("map","true").increment(1);
            return true;
        }
        context.getCounter("map","false").increment(1);
        return false;
    }
}
