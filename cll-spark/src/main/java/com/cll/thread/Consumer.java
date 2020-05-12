package com.cll.thread;

public class Consumer implements Runnable{
    Acount acount;

    Consumer(Acount acount) {
        this.acount = acount;
    }

    public void run() {
        while (true) {
            int random = (int)(Math.random() * 1000);
            acount.getMoney(random);
        }
    }

}