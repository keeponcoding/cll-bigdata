package com.cll.kafka.assignor;

import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @ClassName TestAssignor
 * @Description test kafka assignor
 *
 * @see RangeAssignor
 * @see RoundRobinAssignor
 * @see StickyAssignor
 *
 * @Author cll
 * @Date 2020/1/29 9:48 下午
 * @Version 1.0
 **/
public class TestAssignor {

    public static void main(String[] args) {

        // 每个topic对应的分区数
        Map<String, Integer> partitionsPerTopic = new HashMap<String, Integer>();
        partitionsPerTopic.put("t1",3);
        partitionsPerTopic.put("t2",3);
        partitionsPerTopic.put("t3",3);

        // 消费者
        ArrayList<String> consumers = new ArrayList<String>();
        consumers.add("c1");
        consumers.add("c2");

        // RangeAssignor
        RangeAssignor rangeAssignor = new RangeAssignor();
        kafkaAssignor(rangeAssignor, partitionsPerTopic, consumers);

        // RoundRobinAssignor
        RoundRobinAssignor roundRobinAssignor = new RoundRobinAssignor();
        kafkaAssignor(roundRobinAssignor, partitionsPerTopic, consumers);

        // StickyAssignor
        StickyAssignor stickyAssignor = new StickyAssignor();
        kafkaAssignor(stickyAssignor, partitionsPerTopic, consumers);

    }

    /**
     * RangeAssignor
     */
    private static void kafkaAssignor(AbstractPartitionAssignor assignor, Map<String, Integer> partitionsPerTopic, ArrayList<String> consumers) {

        System.out.println("current assignor name is " + assignor.name() + "...");

        Map<String, PartitionAssignor.Subscription> subscriptions = new HashMap<String, PartitionAssignor.Subscription>();

        Set<String> topicset = partitionsPerTopic.keySet();

        List<String> topics = new ArrayList<String>();
        topics.addAll(topicset);

        PartitionAssignor.Subscription subscription = new PartitionAssignor.Subscription(topics);

        for (String consumer : consumers) {
            subscriptions.put(consumer, subscription);
        }

        Map<String, List<TopicPartition>> assign = assignor.assign(partitionsPerTopic, subscriptions);

        for(String consumer : assign.keySet()){
            System.out.print(consumer + " : ");
            List<TopicPartition> topicPartitions = assign.get(consumer);
            for(TopicPartition tp : topicPartitions){
                System.out.print("["+tp+"]");
            }
            System.out.println();
        }

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\t\n");

    }

}
