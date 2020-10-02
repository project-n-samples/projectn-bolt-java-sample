package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;

public class BoltGetObjHandler implements RequestHandler<Map<String,String>, Boolean> {

    @Override
    public Boolean handleRequest(Map<String,String> event, Context context) {

        String bucket = event.get("bucket");
        String key = event.get("key");

        S3Client s3 = BoltS3Client.builder()
                .build();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();

        try {
            String response = s3.getObject(getObjectRequest, ResponseTransformer.toBytes()).asUtf8String();
            System.out.println(response);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return false;
        }
        return true;
    }
}
