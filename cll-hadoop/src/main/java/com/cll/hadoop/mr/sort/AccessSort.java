package com.cll.hadoop.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName AccessSort
 * @Description 访问日志 实体类
 *     该类需要实现 Writable 接口 并实现 write readFields 方法
 *
 * @Author cll
 * @Date 2020-01-17 22:17
 * @Version 1.0
 **/
public class AccessSort implements WritableComparable<AccessSort> {

    private String phone;
    private Long up;
    private Long down;
    private Long sum;

    /*
     * 该空构造必须加上
     */
    public AccessSort() {
    }

    public AccessSort(String phone, Long up, Long down){
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    /*
     * 序列化
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    /*
     * 反序列化
     *
     * ★ 必须要和 write 写出的字段顺序一致
     */
    public void readFields(DataInput in) throws IOException {
        phone = in.readUTF();
        up = in.readLong();
        down = in.readLong();
        sum = in.readLong();
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return phone + '\t' + up + "\t" + down + "\t" + sum;
    }

    public int compareTo(AccessSort o) {
        return this.getSum() > o.sum ? -1 : 1;
    }
}
