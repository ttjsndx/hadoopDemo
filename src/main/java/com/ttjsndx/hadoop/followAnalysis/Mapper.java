package com.ttjsndx.hadoop.followAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean(); // new一次对象,多次使用. 效率较高

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString() + "";
        String[] fields = line.split("\t");

        String imei = fields[0];
        String time = fields[1];
        long longTime = getInt(time);
        long latitude = getInt(fields[2]);
        long longtitude = getInt(fields[3]);
//        long high = getInt(fields[5]);
//        long speed = getInt(fields[6]);
//        long direction = getInt(fields[7]);

        //1.骑行时间过滤，根据目标车辆的每日骑行map，检测相同时间的点,对比两者的活跃骑行时间是否一致
        String dateKey = time.substring(0,Driver.subTimeLength);
        LinkedList list = Driver.runTimeMap.get(dateKey);
            //1.1遍历list，判断当前记录点是否在list的活跃区段

        //2.距离过滤
        long distinct = checkHandred(longTime,latitude,longtitude);
        if(distinct > 100){
            return;
        }
        //将正确点根据imei号归类，交给reduce处理
        k.set(imei);
        v.set(distinct,longTime,0);
        context.write(k,v);
    }

    /**
     * 计算 与目标车辆的距离
     * @param longTime
     * @param latitude
     * @param longtitude
     * @return
     */
    private long checkHandred(long longTime, long latitude, long longtitude) {
        int[] arr = getPointMap(longTime);
        long district = (latitude - arr[0])*(longtitude - arr[1]);
        return district;
    }

    private int[] getPointMap(long longTime) {
        return new int[2];
    }

    private long getInt(String value){
        return Long.parseLong(value);
    }

}
