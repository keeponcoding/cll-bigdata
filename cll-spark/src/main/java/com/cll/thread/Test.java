package com.cll.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * @ClassName Test
 * @Description TODO
 * @Author cll
 * @Date 2020/5/9 7:58 上午
 * @Version 1.0
 **/
public class Test {

    volatile static int a = 0;

    public static void main(String[] args) {
        new Thread(){
            @Override
            public void run() {
                a++;
                System.out.println("t1 ---> " + a);
            }
        }.start();

        new Thread(){
            @Override
            public void run() {
                a++;
                System.out.println("t2 ---> " + a);
            }
        }.start();

        //callableWithResult();
    }

    private static void callableWithResult() {
        Callable<String> callable = new Callable<String>() {
            public String call() throws Exception {
                // ① 该处睡眠时长 > ②处睡眠时长
                Thread.sleep(1000);
                return "callable";
            }
        };
        FutureTask<String> future = new FutureTask<String>(callable);
        new Thread(future).start();

        try {
            // ②
            Thread.sleep(1000);
            System.out.println("begin...");
            System.out.println(future.isDone());
            future.cancel(true);
            if (!future.isCancelled()) {
                System.out.println(future.get());
                System.out.println(future.isDone());
                System.out.println("end...");
            } else {
                System.out.println("cancel~");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

}
