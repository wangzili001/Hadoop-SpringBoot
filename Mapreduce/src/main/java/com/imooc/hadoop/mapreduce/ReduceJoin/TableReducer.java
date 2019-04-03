package com.imooc.hadoop.mapreduce.ReduceJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.beans.BeanUtils;

import java.io.IOException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //存储所有订单集合
        ArrayList<TableBean> tableBeans = new ArrayList<TableBean>();
        //存储产品信息
        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            //订单表
            if ("order".equals(value.getFlag())) {
                TableBean tmpTableBean = new TableBean();
                BeanUtils.copyProperties(value, tmpTableBean);
                tableBeans.add(tmpTableBean);
            }else {
                BeanUtils.copyProperties(value,pdBean);
            }
        }
        for (TableBean tableBean : tableBeans) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean,NullWritable.get());
        }
    }
}
