package com.cll.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ClassName ThreadPoolApp
 * @Description TODO
 * @Author cll
 * @Date 2020-01-17 11:34
 * @Version 1.0
 **/
public class ThreadPoolApp {

    public static void main(String[] args) {
        // 创建线程池
        ExecutorService threadPool = Executors.newFixedThreadPool(10);
        while(true) {
            threadPool.execute(new Runnable() { // 提交多个线程任务，并执行
                public void run() {
                    System.out.println(Thread.currentThread().getName() + " is running ..");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

}
