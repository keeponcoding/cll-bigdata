package com.cll.thread.share;

/**
 * @ClassName MultiThreadTest
 * @Description TODO
 * @Author cll
 * @Date 2020/5/12 3:05 下午
 * @Version 1.0
 **/
public class MultiThreadTest {

    public static void main(String[] args) {
        final Bank bank = new Bank();
        new Thread("father"){
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    bank.setMoney(Math.round(Math.random())*100);
                    System.out.println(bank.queryMoney());
                }
            }
        }.start();
        new Thread("son"){
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    // wait();
                    bank.getMoney(Math.round(Math.random())*100);
                    System.out.println(bank.queryMoney());
                }
            }
        }.start();
    }
}
