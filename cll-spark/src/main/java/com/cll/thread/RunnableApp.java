package com.cll.thread;

/**
 * @ClassName ImplementThreadApp
 * @Description TODO
 * @Author cll
 * @Date 2020-01-17 11:07
 * @Version 1.0
 **/
public class RunnableApp implements Runnable {

    public void run() {
        System.out.println("当前线程名称：" + Thread.currentThread().getName() + "，implement Runnable...");
    }

    public static void main(String[] args) {

        RunnableApp app1 = new RunnableApp();
        RunnableApp app2 = new RunnableApp();
        RunnableApp app3 = new RunnableApp();

        new Thread(app1).start();
        new Thread(app2).start();
        new Thread(app3).start();

        System.out.println("main方法print:" + Thread.currentThread().getName());
    }
}
