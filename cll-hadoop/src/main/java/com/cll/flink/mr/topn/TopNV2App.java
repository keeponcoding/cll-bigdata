package com.cll.flink.mr.topn;

import com.cll.flink.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName TopNApp
 * @Description map reduce
 * @Author cll
 * @Date 2020-01-17 15:10
 * @Version 1.0
 **/
public class TopNV2App {

    public static void main(String[] args) throws Exception{
        // STEP 1 initial Configuration  get job instance
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // STEP 2 set jar info
        job.setJarByClass(TopNV2App.class);

        // STEP 3 set custome Mapper Reducer
       job.setMapperClass(MyMapper.class);
       job.setReducerClass(MyReducer.class);

        // STEP 4 set Mapper output key/value type
        job.setMapOutputKeyClass(CllIntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // STEP 5 set Reducer output key/value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CllIntWritable.class);

        // STEP 6 set input output path
        String input = "cll-hadoop/data/name.data";
        String output = "cll-hadoop/data/output/";

        FileUtil.deleteTarget(output,conf);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // STEP 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

    private static final int TOPN = 3;

    public static class CllIntWritable extends IntWritable {

        public CllIntWritable(){}

        public CllIntWritable(int value){super(value);}

        @Override
        public int compareTo(IntWritable o) {
            return -super.compareTo(o); // 不加 -  默认升序
        }
    }

    /**
     * KEYIN       输入数据key的数据类型       每行数据的偏移量
     * VALUEIN     输入数据value的数据类型     每行数据内容
     * KEYOUT      输出数据key的数据类型       输出的每个单词
     * VALUEOUT    输出数据value的数据类型     输出的每个单词的次数
     *
     * MyMapper 用static修饰
     */
    public static class MyMapper extends Mapper<LongWritable, Text, CllIntWritable, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Text value 就是每行数据
            String[] splits = value.toString().split(",");

            String name = splits[0];
            int age = Integer.valueOf(splits[1]);

            context.write(new CllIntWritable(age), new Text(name));

        }
    }

    /**
     * reducer 的输入就是 Mapper 的输出
     *
     * MyReducer 用static修饰
     */
    public static class MyReducer extends Reducer<CllIntWritable, Text, Text, CllIntWritable>{

        int index = 0;

        @Override
        protected void reduce(CllIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text value : values){
                if(index < TOPN){
                    context.write(value, key);
                    index++;
                }
            }
        }
    }

}
