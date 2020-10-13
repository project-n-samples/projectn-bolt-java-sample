package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BoltS3ListObjsV2Handler implements RequestHandler<Map<String,String>, List<String>> {

    @Override
    public List<String> handleRequest(Map<String,String> event, Context context) {

        String bucketName = event.get("bucket");
        String requestType = event.get("requestType");

        S3Client s3 = null;
        if (requestType != null && !requestType.isEmpty()) {
            if (requestType.equals("bolt")) {
                s3 = BoltS3Client.builder().build();
            } else if (requestType.equals("s3")) {
                s3 = S3Client.builder().build();
            }
        } else {
            s3 = BoltS3Client.builder().build();
        }

        List<String> objList = new ArrayList<>();
        try {
            ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(bucketName).build();
            ListObjectsV2Response resp;

            resp = s3.listObjectsV2(req);

            List<S3Object> objects = resp.contents();

            for (S3Object object : objects) {
                objList.add(object.key());
            }
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return objList;
    }
}
