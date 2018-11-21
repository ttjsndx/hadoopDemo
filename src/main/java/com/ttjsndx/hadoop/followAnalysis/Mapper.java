package com.ttjsndx.hadoop.followAnalysis;

import com.ttjsndx.hadoop.flowsum.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean(); // new一次对象,多次使用. 效率较高

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString() + "";
        String[] fields = line.split("\t");

        String imei = fields[0];
        String time = fields[1];
        String latitude = fields[2];
        String longtitude = fields[3];
        String high = fields[5];
        String speed = fields[6];
        String direction = fields[7];



        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);

        //使用对象中的set方法来写入数据,避免大量new对象
        k.set(phoneNum);
        v.set(upFlow,downFlow);
        context.write(k,v);
    }
}
