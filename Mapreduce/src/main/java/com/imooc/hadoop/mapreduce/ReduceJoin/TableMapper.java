package com.imooc.hadoop.mapreduce.ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text,TableBean> {
    String name;
    TableBean tableBean = new TableBean();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件的名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        id     pid amount
//        1001    01  1001
//        pid pname
//        01  小米
        //获取一行
        String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
        String[] split = line.split("\t");
        //订单表0
        if(name.startsWith("order")){
            tableBean.setId(split[0]);
            tableBean.setPid(split[1]);
            tableBean.setAmount(Integer.parseInt(split[2]));
            tableBean.setPname("");
            tableBean.setFlag("order");
            k.set(split[1]);
        }else {
            tableBean.setId("");
            tableBean.setPid(split[0]);
            tableBean.setAmount(0);
            tableBean.setPname(split[1]);
            tableBean.setFlag("pb");
            k.set(split[0]);
        }
        context.write(k,tableBean);
    }
}
