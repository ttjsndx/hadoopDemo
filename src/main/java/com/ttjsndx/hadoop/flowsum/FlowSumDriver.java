package com.ttjsndx.hadoop.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowSumDriver {
    public static void main(String[] args) throws Exception{
        long l = System.currentTimeMillis();
        //通过Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        //conf.set("mapreduce.framework.name","local");
        Job job = Job.getInstance(conf);

        //指定本次mr job jar包运行主类
        job.setJarByClass(FlowSumDriver.class);

        //指定本次mr 所用的mapper reducer类分别是什么
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        //指定本次mr mapper阶段的输出  k  v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定本次mr 输入的数据路径 和最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"D:\\hdfs\\flowsum\\input\\1.txt");
        FileOutputFormat.setOutputPath(job,new Path("D:\\hdfs\\flowsum\\output"));
        //FileInputFormat.setInputPaths(job,"/wordcount/input");
        //FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));

        // job.submit(); //一般不要这个.
        //提交程序  并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.out.println("花费时间：" + (System.currentTimeMillis() - l));
        System.exit(b?0:1);
    }
}