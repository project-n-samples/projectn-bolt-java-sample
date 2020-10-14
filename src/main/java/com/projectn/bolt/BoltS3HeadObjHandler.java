package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;
import java.util.HashMap;

public class BoltS3HeadObjHandler implements RequestHandler<Map<String,String>, Map<String,String>> {

    @Override
    public Map<String,String> handleRequest(Map<String,String> event, Context context) {

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
        try {
            HeadObjectResponse res = s3.headObject(headObjectRequest);
            Map<String,String> map = new HashMap<String, String>() {{
                put( "Expiration", res.expiration());
                put( "lastModified", res.lastModified().toString() );
                put( "ContentLength", res.contentLength().toString() );
                put( "ETag", res.eTag() );
                put( "VersionId", res.versionId() );
                put( "StorageClass", res.storageClass().toString() );
            }};
            return map;
        } catch (S3Exception e) {
            String msg = e.awsErrorDetails().errorMessage();
            System.err.println(msg);
            Map<String,String> map = new HashMap<String, String>() {{
                put("errorMessage", msg);
            }};
            return map;
        }
    }
}
