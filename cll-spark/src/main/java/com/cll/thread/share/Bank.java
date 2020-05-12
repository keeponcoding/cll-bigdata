package com.cll.thread.share;

/**
 * @ClassName Bank
 * @Description TODO
 * @Author cll
 * @Date 2020/5/12 2:59 下午
 * @Version 1.0
 **/
public class Bank {

    private static double acount = 0;

    // 存钱
    public void setMoney(double money) {
        System.out.println(Thread.currentThread().getName()+"往账户存入:"+money+"，账户剩余："+ (acount += money));
    }

    // 取钱
    public void getMoney(double money) {
        if (money > acount) {
            System.out.println("当前账户剩余:" + acount + "，不足以取:"+money);
        }else{
            System.out.println(Thread.currentThread().getName()+"取出:"+money+"，账户剩余："+ (acount -= money));
        }
    }

    // 查询余额
    public double queryMoney() {
        return acount;
    }
}
