package com.ttjsndx.hadoop.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    //写上这句{}之后,点击Writable,点击左边的灯泡，Implement Mothods 选中write和readFields后OK。
    // Hadoop的Writable 比JAVA自带的要高效很多。
    //手工输入，定义三个属性
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    // R click blank, Generator , Constructor, Select None 生成无参构造函数
    public FlowBean() {
    }

    //右击类名,Generate, Constructor,选中所有子项，OK，生成有参构造函数
    // R click blank, Generator , Constructor, select All, OK
    public FlowBean(long upFlow, long downFlow, long sumFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    //自己构造一个2个参数的方法。
    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    //自己构造一个有返回值的 set方法，
    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    //这是序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sumFlow;
    }

    //这是反序列化方法
    //反序列化时候, 注意序列化的顺序
    // 先序列化的先出来.
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    // 右击类名,Generate, Getter and Setter, 选中全部，OK
    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}