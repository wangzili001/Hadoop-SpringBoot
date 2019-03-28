package com.imooc.hadoop.mapreduce.PhonePayLoadSortMapReduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private String phoneNB;
    private long up_flow;
    private long down_flow;
    private long sum_flow;

    // 在反序列化时，反射机制需要调用空参构造函数，所以显示定义了一个空参构造函数
    public FlowBean() {
    }

    // 为了对象数据的初始化方便，加入一个带参的构造函数
    public FlowBean(String phoneNB, long up_flow, long down_flow) {
        this.phoneNB = phoneNB;
        this.up_flow = up_flow;
        this.down_flow = down_flow;
        this.sum_flow = up_flow + down_flow;
    }
    // 将对象的数据序列化到流中
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNB);
        out.writeLong(up_flow);
        out.writeLong(down_flow);
        out.writeLong(sum_flow);
    }

    // 从流中反序列化出对象的数据
    // 从数据流中读出对象字段时，必须跟序列化时的顺序保持一致
    public void readFields(DataInput in) throws IOException {
        this.phoneNB = in.readUTF();
        this.up_flow = in.readLong();
        this.down_flow = in.readLong();
        this.sum_flow = in.readLong();
    }

    // 实现Comparable接口，需要复写compareTo方法
    public int compareTo(FlowBean flowBean) {
        return this.sum_flow > flowBean.sum_flow ? -1 : 1;
    }

    public String getPhoneNB() {
        return phoneNB;
    }

    public void setPhoneNB(String phoneNB) {
        this.phoneNB = phoneNB;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getDown_flow() {
        return down_flow;
    }

    public void setDown_flow(long down_flow) {
        this.down_flow = down_flow;
    }

    public long getSum_flow() {
        return sum_flow;
    }

    public void setSum_flow(long sum_flow) {
        this.sum_flow = sum_flow;
    }
    @Override
    public String toString() {
        return "" + up_flow + "\t" + down_flow + "\t" + sum_flow;
    }
}
