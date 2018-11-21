package com.ttjsndx.hadoop.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

    Text k = new Text();
    FlowBean v = new FlowBean(); // new一次对象,多次使用. 效率较高
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t"); //数据文件如果是两个连续的 \t 会有问题.

        String phoneNum = fields[1];
        long upFlow = Long.parseLong(fields[fields.length-3]); //倒着获取需要的字段
        long downFlow = Long.parseLong(fields[fields.length-2]);

        // context.write(new Text(phoneNum),new FlowBean(upFlow,downFlow)); //效率太低,每次都产生对象.

        //使用对象中的set方法来写入数据,避免大量new对象
        k.set(phoneNum);
        v.set(upFlow,downFlow);
        context.write(k,v);
    }
}