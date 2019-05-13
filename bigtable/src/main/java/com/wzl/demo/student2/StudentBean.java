package com.wzl.demo.student2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentBean implements WritableComparable<StudentBean> {
    private String course; //课程名
    private String name; //学生姓名
    private float avg; //平均分

    public int compareTo(StudentBean student) {
        return this.avg>student.getAvg()?1:-1;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(course);
        out.writeUTF(name);
        out.writeFloat(avg);
    }

    public void readFields(DataInput in) throws IOException {
        course = in.readUTF();
        name = in.readUTF();
        avg = in.readFloat();
    }

    public StudentBean(String course, String name, float avg) {
        this.course = course;
        this.name = name;
        this.avg = avg;
    }

    public StudentBean() {
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getAvg() {
        return avg;
    }

    public void setAvg(float avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {
        return course+"\t"+name+"\t"+avg;
    }
}
