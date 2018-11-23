package com.ttjsndx.hadoop.followAnalysis;

import com.ttjsndx.hadoop.mr.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;
import java.util.LinkedList;


public class Driver {

    //subTimeLength 的取值以后再定，要定到凌晨2-4点活动少的时候
    public static int subTimeLength = 8;

    public static HashMap<String, LinkedList> runTimeMap = new HashMap();
    public static void main(String[] args) {
        Driver driver = new Driver();

        //1.计算目标车辆每一天的骑行时间段
        runTimeMap = driver.initRunTime();

        try {
            driver.initHadoop();
        }catch (Exception e){
            System.out.println("启动hadoop job失败:" + e);
            return;
        }
    }

    /**
     * 截取0到subTimeLength位的数字作为时间段，获取时间段内的骑行时间
     * @return
     */
    private HashMap initRunTime() {
        HashMap map = new HashMap();
        map.put("",1);
        return map;
    }

    private void initHadoop() throws Exception {
        //通过Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //指定本次mr job jar包运行主类
        job.setJarByClass(Driver.class);

        //指定本次mr 所用的mapper reducer类分别是什么
        job.setMapperClass(Mapper.class);  //根据时间和区域第一次过滤数据并根据imei号分组
        job.setCombinerClass(Combiner.class); //根据小组内的时间段进行更细的分组，并去除掉时间段太短的分组
        job.setReducerClass(Reduce.class);  //排序分组

        //指定本次mr mapper阶段的输出  k  v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定ReduceTask个数
        //job.setNumReduceTasks(100);

        //指定本次mr 输入的数据路径 和最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"D:\\hadoopDateSpace\\followsum\\input\\1.txt");
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoopDateSpace\\followsum\\output"));

        //提交程序  并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
