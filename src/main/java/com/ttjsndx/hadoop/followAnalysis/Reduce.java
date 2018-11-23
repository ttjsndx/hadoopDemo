package com.ttjsndx.hadoop.followAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class  Reduce extends Reducer<Text,FlowBean,Text,FlowBean>{

    FlowBean v = new FlowBean();

    protected void reduce(Text key, Iterable<FlowBean> values, Reducer.Context context) throws IOException, InterruptedException {

        for(FlowBean bean:values){
            bean.getDistrict();
            bean.getDownFlow();
        }
        v.set(upFlowCount,downFlowCount);
        context.write(key,v);
    }
}
