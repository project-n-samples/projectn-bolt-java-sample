package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;
import java.util.HashMap;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BoltS3GetObjHandler implements RequestHandler<Map<String,String>, Map<String,String>> {

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

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
        try {

            byte[] res = s3.getObject(getObjectRequest, ResponseTransformer.toBytes()).asByteArray();

            // Parse the MD5 of the returned object
            MessageDigest md = MessageDigest.getInstance("MD5"); 
            md.update(res);
	        String md5 = DatatypeConverter
            .printHexBinary(md.digest()).toUpperCase();

            Map<String,String> map = new HashMap<String, String>() {{
                put("md5", md5);
            }};
            return map;
        } catch (S3Exception e) {
            String msg = e.awsErrorDetails().errorMessage();
            System.err.println(msg);
            Map<String,String> map = new HashMap<String, String>() {{
                put("errorMessage", msg);
            }};
            return map;
        } catch (NoSuchAlgorithmException e) {
            String msg = e.toString();
            System.err.println(msg);
            Map<String,String> map = new HashMap<String, String>() {{
                put("errorMessage", msg);
            }};
            return map;
        }
    }
}

    