package com.cll.spark.cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.*;
import com.qcloud.cos.region.Region;

import java.io.File;
import java.util.List;

/**
 * @ClassName COSAPIApp
 * @Description 操作cos 腾讯云 对象存储
 * @Author cll
 * @Date 2020-01-07 21:50
 * @Version 1.0
 **/
public class COSAPIApp {

    public static void main(String[] args) {

        //System.out.println(getCOSClient());
        //createBucket();
        //listBucket();
        //doesBucketExist("cll1-1257466126");
        //deleteBucket("cll1-1257466126");
        upload("cll-1257466126","cll-spark/data/people.csv");
    }

    public static void download(String bucketName, String key){
        COSClient cosClient = getCOSClient();

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        COSObject object = cosClient.getObject(getObjectRequest);


        cosClient.shutdown();
    }

    /**
     * 上传
     * @param bucketName
     * @param pathName
     */
    public static void upload(String bucketName, String pathName){
        COSClient cosClient = getCOSClient();

        File file = new File(pathName);
        String key = "peoplecsv";
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,key,file);
        cosClient.putObject(putObjectRequest);

        cosClient.shutdown();
    }

    /**
     * 删除桶
     * @param bucketName
     */
    public static void deleteBucket(String bucketName){
        COSClient cosClient = getCOSClient();

        cosClient.deleteBucket(bucketName); // 如果桶内有对象  会抛出异常

        cosClient.shutdown();
    }

    /**
     * 判断bucket是否存在
     * @param bucketName
     */
    public static void doesBucketExist(String bucketName){
        COSClient cosClient = getCOSClient();

        boolean bucketExist = cosClient.doesBucketExist(bucketName);

        System.out.println(bucketExist);

        cosClient.shutdown();
    }

    /**
     * 查询桶
     */
    public static void listBucket(){
        COSClient cosClient = getCOSClient();
        List<Bucket> buckets = cosClient.listBuckets();
        for (Bucket bucket : buckets) {
            String location = bucket.getLocation(); // 位置
            String name = bucket.getName(); // 名称
            System.out.println(name + "--->" + location);
        }
        cosClient.shutdown();
    }

    /**
     * 创建桶
     */
    public static void createBucket(){
        COSClient cosClient = getCOSClient();

        String bucketName = "cll1-1257466126";
        CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName);

        cosClient.createBucket(bucketName);

        cosClient.shutdown();
    }

    /**
     * 获取 cosclient
     * @return
     */
    public static COSClient getCOSClient(){
        String regionName = "ap-shanghai";
        COSCredentials cred = new BasicCOSCredentials(SecretConstants.accessKey, SecretConstants.secretKey);

        Region region = new Region(regionName);
        ClientConfig clientConfig = new ClientConfig(region);
        COSClient cosClient = new COSClient(cred,clientConfig);
        return cosClient;
    }

}
