package com.cll.thread;

/**
 * @ClassName ExtendsThreadApp
 * @Description TODO
 * @Author cll
 * @Date 2020-01-17 11:01
 * @Version 1.0
 **/
public class ExtendsThreadApp extends Thread {

    public static void main(String[] args) {
        ExtendsThreadApp app1 = new ExtendsThreadApp();
        ExtendsThreadApp app2 = new ExtendsThreadApp();
        ExtendsThreadApp app3 = new ExtendsThreadApp();

        // 启动线程
        app1.start();
        app2.start();
        app3.start();

        System.out.println("主线程：" + Thread.currentThread().getName());
    }

    @Override
    public void run() {
        System.out.println("当前线程：" + Thread.currentThread().getName());
    }
}
