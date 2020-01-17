package com.cll.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @ClassName CallableThreadApp
 * @Description TODO
 * @Author cll
 * @Date 2020-01-17 11:23
 * @Version 1.0
 **/
public class CallableThreadApp implements Callable {

    private String name;

    public CallableThreadApp(String threadName){
        this.name = threadName;
    }

    public Object call() throws Exception {
        return this.name;
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        int taskSize = 10;
        // 创建一个线程池
        ExecutorService pool = Executors.newFixedThreadPool(taskSize);

        // 初始化一个存放future的集合
        List<Future> list = new ArrayList<Future>();

        for (int i = 0; i < taskSize; i++){
            Callable app = new CallableThreadApp("t-"+i);
            Future future = pool.submit(app);
            list.add(future);
        }

        // 关闭线程池
        pool.shutdown();

        // 遍历Future结果
        for (Future f : list){
            System.out.println("result ---> " + f.get().toString());
        }


    }
}
