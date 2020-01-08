package com.cll.spark.cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.Bucket;
import com.qcloud.cos.model.CreateBucketRequest;
import com.qcloud.cos.region.Region;

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
        listBucket();
    }

    /**
     * 查询桶
     */
    public static void listBucket(){
        COSClient cosClient = getCOSClient();
        List<Bucket> buckets = cosClient.listBuckets();
        for (Bucket bucket : buckets) {
            String location = bucket.getLocation();
            String name = bucket.getName();
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
