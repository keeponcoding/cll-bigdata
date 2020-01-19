package com.cll.hadoop.mr.partitioner;

import com.cll.hadoop.domain.AccessLog;
import com.cll.hadoop.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName AccessGlobalSortApp
 * @Description TODO
 * @Author cll
 * @Date 2020-01-17 22:17
 * @Version 1.0
 **/
public class AccessLogCustomePartionerApp {

    public static void main(String[] args) throws Exception {
        // STEP 1 initial Configuration  get job instance
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // STEP 2 set jar info
        job.setJarByClass(AccessLogCustomePartionerApp.class);

        // STEP 3 set custome Mapper Reducer
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(AccessLogReducer.class);

        // STEP 4 set Mapper output key/value type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AccessLog.class);

        // STEP 5 set Reducer output key/value type
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(AccessLog.class);

        // STEP 6 set input output path
        String input = "cll-hadoop/data/access.log";
        String output = "cll-hadoop/data/output/";

        FileUtil.deleteTarget(output,conf);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // reducer数 可以设置大于 分区数
        //           不可以设置小于 分区数 但是可以设置为1
        job.setPartitionerClass(AccessLogPartitioner.class); // 设置自定义分区器
        job.setNumReduceTasks(3); // 设置reducer数

        // STEP 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

    /*
     *
     */
    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, AccessLog>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split(" ");
            String phone = splits[0];
            long down = Long.valueOf(splits[splits.length-3]);
            long up = Long.valueOf(splits[splits.length-2]);
            context.write(new Text(phone),new AccessLog(phone,up,down));
        }
    }

    /*
     *
     */
    public static class AccessLogReducer extends Reducer<Text, AccessLog, NullWritable, AccessLog>{

        @Override
        protected void reduce(Text key, Iterable<AccessLog> values, Context context) throws IOException, InterruptedException {
            // 初始化 上限 下限
            long ups = 0;
            long downs = 0;

            // 累加操作
            for (AccessLog accessLog : values){
                ups += accessLog.getUp();
                downs += accessLog.getDown();
            }

            context.write(NullWritable.get(), new AccessLog(key.toString(), ups, downs));
        }
    }

}
