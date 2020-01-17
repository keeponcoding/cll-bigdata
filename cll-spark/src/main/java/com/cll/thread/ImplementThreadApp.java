package com.cll.thread;

/**
 * @ClassName ImplementThreadApp
 * @Description TODO
 * @Author cll
 * @Date 2020-01-17 11:07
 * @Version 1.0
 **/
public class ImplementThreadApp implements Runnable {

    public void run() {
        System.out.println("当前线程名称：" + Thread.currentThread().getName() + "，implement Runnable...");
    }

    public static void main(String[] args) {

        ImplementThreadApp app1 = new ImplementThreadApp();
        Thread t1 = new Thread(app1);

        ImplementThreadApp app2 = new ImplementThreadApp();
        Thread t2 = new Thread(app2);

        ImplementThreadApp app3 = new ImplementThreadApp();
        Thread t3 = new Thread(app3);

        t1.start();
        t2.start();
        t3.start();

        System.out.println("main方法print:" + Thread.currentThread().getName());
    }
}
