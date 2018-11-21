package com.ttjsndx.hadoop.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <手机号1, bean><手机号2, bean><手机号2, bean><手机号, bean>
 *
 *     <手机号1, bean><手机号1, bean>
 *     <手机号2, bean><手机号2, bean>
 */
public class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean>{

    FlowBean v = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlowCount = 0;
        long downFlowCount = 0;

        for(FlowBean bean:values){
            upFlowCount += bean.getUpFlow();
            downFlowCount += bean.getDownFlow();
        }
        v.set(upFlowCount,downFlowCount);
        context.write(key,v);
    }
}