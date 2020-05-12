package com.cll.thread.share;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName ThreadShareData
 * @Description 线程之间共享数据
 *              1.执行代码一致
 *              2.执行代码不一致
 * @Author cll
 * @Date 2020/5/12 9:15 上午
 * @Version 1.0
 **/
public class ThreadShareData implements Runnable{

    private static int a = 0;
    ReentrantLock lock = new ReentrantLock();
    public void run() {
        /*
         * 数据线程安全问题
         * 解决方案：
         * 1.synchronized
         * 2.手动lock
         */
        // 方案1
        /*synchronized (this) {
            a++;
            System.out.println(Thread.currentThread().getName() + " ---> " + a);
        }*/

        // 方案2
        lock.lock(); // 加锁
        a++;
        System.out.println(Thread.currentThread().getName() + " ---> " + a);
        lock.unlock(); // 释放锁
    }
    public static void main(String[] args) {
        ThreadShareData t = new ThreadShareData();
        new Thread(t).start();
        new Thread(t).start();
        new Thread(t).start();
        new Thread(t).start();
        new Thread(t).start();
    }
}
