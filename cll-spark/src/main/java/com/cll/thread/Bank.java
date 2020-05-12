package com.cll.thread;

public class Bank implements Runnable{
    Acount acount;
    public Bank(Acount acount) {
        this.acount = acount;
    }

    public void run() {
        while(true) {
            int random = (int)(Math.random() * 1000);
            acount.setMoney(random);
        }
    }
}