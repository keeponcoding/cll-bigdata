package com.cll.thread;

/**
 * 两个线程执行同一套代码
 * 共享threadCnt变量
 */
public class RunnalbleTest2 implements Runnable {
    private int threadCnt = 10;

    public void run() {
        while (true) {
            if (threadCnt > 0) {
                System.out.println(Thread.currentThread().getName() + " 剩余个数 " + threadCnt);
                threadCnt--;
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                break;
            }
        }
    }

    public static void main(String[] args) {
        RunnalbleTest2 runnalbleTest2 = new RunnalbleTest2();
        new Thread(runnalbleTest2).start();
        new Thread(runnalbleTest2).start();
    }
}