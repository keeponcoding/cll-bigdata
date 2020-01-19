package com.cll.hadoop.mr.topn;

import com.cll.hadoop.util.FileUtil;
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
import java.util.TreeMap;

/**
 * @ClassName TopNApp
 * @Description map reduce
 * @Author cll
 * @Date 2020-01-17 15:10
 * @Version 1.0
 **/
public class TopNApp {

    public static void main(String[] args) throws Exception{
        // STEP 1 initial Configuration  get job instance
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // STEP 2 set jar info
        job.setJarByClass(TopNApp.class);

        // STEP 3 set custome Mapper Reducer
       job.setMapperClass(MyMapper.class);
       //job.setReducerClass(MyReducer.class);

        // STEP 4 set Mapper output key/value type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // STEP 5 set Reducer output key/value type
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(IntWritable.class);

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

    /**
     * KEYIN       输入数据key的数据类型       每行数据的偏移量
     * VALUEIN     输入数据value的数据类型     每行数据内容
     * KEYOUT      输出数据key的数据类型       输出的每个单词
     * VALUEOUT    输出数据value的数据类型     输出的每个单词的次数
     *
     * MyMapper 用static修饰
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        // 自带排序功能的map 从小到大
        TreeMap<Integer,String> map = new TreeMap<Integer, String>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Text value 就是每行数据
            String[] splits = value.toString().split(",");

            String name = splits[0];
            int age = Integer.valueOf(splits[1]);

            map.put(age,name);

            /*
             * 移除 TOPN 之外的元素  只保留TOPN数据
             * 如果倒序取TOPN 那就移除 first
             * 如果正序取TOPN 那就移除 last
             */
            if(map.size() > TOPN){
                map.remove(map.firstKey());
            }

        }

        /*
         * 最后输出 map
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Integer age : map.keySet()){
                context.write(new Text(map.get(age)), new IntWritable(age));
            }
        }
    }

    /**
     * reducer 的输入就是 Mapper 的输出
     *
     * MyReducer 用static修饰
     */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /*
             * Text key  key就是每个单词
             * Iterable<IntWritable> values 相同的key分发到同一个节点上
             *
             * 示例数据
             * 原始数据
             * a,a,a
             * b,b
             * c
             *
             * mapper输出的结果
             * (a,1) (a,1) (a,1)
             * (b,1) (b,1)
             * (c,1)
             *
             * reduce参数 values
             * a,<1,1,1>
             * b,<1,1>
             * c,<1>
             *
             */
            int cnt = 0;

            for (IntWritable i : values) {
                cnt += i.get();
            }

            // System.out.println("...key..."+key);

            context.write(key, new IntWritable(cnt));
        }
    }

}
