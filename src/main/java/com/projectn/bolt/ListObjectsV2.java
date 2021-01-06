package com.projectn.bolt;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

public class ListObjectsV2 {

    public static void main(String []args) {

        if (args.length < 1) {
            System.out.println("Please specify a bucket name");
            System.exit(1);
        }

        String bucketName = args[0];
        S3Client s3 = BoltS3Client.builder().build();

        //S3Client s3 = S3Client.builder()
        //        .build();

        listBucketObjectsV2(s3, bucketName);
    }

    public static void listBucketObjectsV2(S3Client s3, String bucketName ) {

        try {
            ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(bucketName).build();
            ListObjectsV2Response resp;

            resp = s3.listObjectsV2(req);

            List<S3Object> objects = resp.contents();

            for (S3Object object : objects) {
                System.out.println(object.key());
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}

