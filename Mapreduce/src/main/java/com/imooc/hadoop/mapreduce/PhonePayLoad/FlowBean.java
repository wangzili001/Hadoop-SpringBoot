package com.imooc.hadoop.mapreduce.PhonePayLoad;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
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

    public void write(DataOutput output) throws IOException {
        output.writeUTF(phoneNB);
        output.writeLong(up_flow);
        output.writeLong(down_flow);
        output.writeLong(sum_flow);
    }

    public void readFields(DataInput input) throws IOException {
        this.phoneNB = input.readUTF();
        this.up_flow = input.readLong();
        this.down_flow = input.readLong();
        this.sum_flow = input.readLong();
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
        return "\t" + up_flow + "\t" + down_flow + "\t" + sum_flow;
    }
}
