package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;

public class BoltS3HeadObjHandler implements RequestHandler<Map<String,String>, String> {

    @Override
    public String handleRequest(Map<String,String> event, Context context) {

        String bucket = event.get("bucket");
        String key = event.get("key");
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

        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        String response = "";
        try {
            response = s3.headObject(headObjectRequest).toString();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return response;
    }
}
