package com.wzl.demo.student2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CoursePartitioner extends Partitioner<StudentBean, NullWritable> {

    public int getPartition(StudentBean studentBean, NullWritable nullWritable, int i) {
        if("algorithm".equals(studentBean.getCourse())){
            return 0;
        }else if("computer".equals(studentBean.getCourse())){
            return 1;
        }else if("english".equals(studentBean.getCourse())){
            return 2;
        }else{
            return 3;
        }
    }
}
