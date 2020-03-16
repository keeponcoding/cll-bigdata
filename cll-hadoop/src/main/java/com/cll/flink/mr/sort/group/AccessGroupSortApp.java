package com.cll.flink.mr.sort.group;

import com.cll.flink.mr.sort.AccessSort;
import com.cll.flink.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName AccessGlobalSortApp
 * @Description 分组排序
 * @Author cll
 * @Date 2020-01-17 22:17
 * @Version 1.0
 **/
public class AccessGroupSortApp {

    public static void main(String[] args) throws Exception {
        // STEP 1 initial Configuration  get job instance
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // STEP 2 set jar info
        job.setJarByClass(AccessGroupSortApp.class);

        // STEP 3 set custome Mapper Reducer
        job.setMapperClass(AccessGroupSortMapper.class);
        job.setReducerClass(AccessGroupSortReducer.class);

        // STEP 4 set Mapper output key/value type
        job.setMapOutputKeyClass(AccessSort.class);
        job.setMapOutputValueClass(Text.class);

        // STEP 5 set Reducer output key/value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AccessSort.class);

        // STEP 6 set input output path
        String input = "cll-hadoop/data/access.log";
        String output = "cll-hadoop/data/output/";

        FileUtil.deleteTarget(output,conf);

        // 设置自定义分区器
        job.setPartitionerClass(AccessPartitioner.class);
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // STEP 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

    /*
     * 排序 需要根据 key 进行排序
     * 所以Mapper的输出 就是自定义类
     */
    public static class AccessGroupSortMapper extends Mapper<LongWritable, Text, AccessSort, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(" ");
            String phone = splits[0];
            long down = Long.valueOf(splits[splits.length-3]);
            long up = Long.valueOf(splits[splits.length-2]);
            context.write(new AccessSort(phone,up,down), new Text(phone));
        }
    }

    /*
     *
     */
    public static class AccessGroupSortReducer extends Reducer<AccessSort, Text, Text, AccessSort>{

        @Override
        protected void reduce(AccessSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values){
                context.write(value, key);
            }
        }
    }

}
