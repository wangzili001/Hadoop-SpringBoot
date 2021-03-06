package com.imooc.hadoop.mapreduce.OrderSortByGroup;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 001	a1	125
 * 002	a1	125
 * 001	b2	258
 * 003	a2	200
 * 003	c1	500
 * 002	c2	775
 */
public class Order implements WritableComparable<Order> {
    private int id;
    private int price;

    public int compareTo(Order o) {
        int result;
        //先按订单升序 再按价格降序
        if(id<o.id){
            result = -1;
        }else if(id>o.id){
            result = 1;
        }else {
            result = price>o.price?-1:1;
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(price);
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        price = in.readInt();
    }

    public  Order(){
    }

    public  Order(int id,int price){
        this.id = id;
        this.price = price;
    }

    @Override
    public String toString() {
        return id + "\t" + price;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }
}
