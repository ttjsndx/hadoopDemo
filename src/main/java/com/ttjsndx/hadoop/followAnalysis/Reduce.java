package com.ttjsndx.hadoop.followAnalysis;

import com.ttjsndx.hadoop.flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class  Reduce extends Reducer<Text,FlowBean,Text,FlowBean>{

    FlowBean v = new FlowBean();

    protected void reduce(Text key, Iterable<FlowBean> values, Reducer.Context context) throws IOException, InterruptedException {
        long upFlowCount = 1;
        long downFlowCount = 1;

        for(FlowBean bean:values){
            upFlowCount += bean.getUpFlow();
            downFlowCount += bean.getDownFlow();
        }
        v.set(upFlowCount,downFlowCount);
        context.write(key,v);
    }
}
