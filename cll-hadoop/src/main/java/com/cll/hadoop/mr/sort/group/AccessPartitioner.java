package com.cll.hadoop.mr.sort.group;

import com.cll.hadoop.domain.AccessLog;
import com.cll.hadoop.mr.sort.AccessSort;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName AccessLogPartitioner
 * @Description 分区器
 *
 * Partitioner<>  后面跟的范型 需要与mapper输出类型保持一致
 *
 * @Author cll
 * @Date 2020-01-19 13:33
 * @Version 1.0
 **/
public class AccessPartitioner extends Partitioner<AccessSort, Text> {

    @Override
    public int getPartition(AccessSort accessSort, Text phone, int numPartitions) {
        String phoneNum = phone.toString();

        if(phoneNum.startsWith("13")){
            return 0;
        }else if(phoneNum.startsWith("15")){
            return 1;
        }else{
            return 2;
        }
    }
}
