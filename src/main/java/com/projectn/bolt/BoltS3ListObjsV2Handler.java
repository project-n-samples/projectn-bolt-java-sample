package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

public class BoltS3ListObjsV2Handler implements RequestHandler<String, Boolean> {

    @Override
    public Boolean handleRequest(String event, Context context) {

        String bucketName = event;
        S3Client s3 = BoltS3Client.builder()
                .build();

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
            return false;
        }
        return true;
    }
}
