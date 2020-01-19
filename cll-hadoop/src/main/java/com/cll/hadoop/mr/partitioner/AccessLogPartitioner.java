package com.cll.hadoop.mr.partitioner;

import com.cll.hadoop.domain.AccessLog;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName AccessLogPartitioner
 * @Description 分区器
 * @Author cll
 * @Date 2020-01-19 13:33
 * @Version 1.0
 **/
public class AccessLogPartitioner extends Partitioner<Text, AccessLog> {

    public int getPartition(Text phone, AccessLog accessLog, int numPartitions) {
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
