package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BoltS3OpsHandler implements RequestHandler<Map<String,String>, Map<String,String>> {

    enum RequestType {
        HEAD_OBJECT,
        GET_OBJECT,
        LIST_OBJECTS_V2
    }

    enum SdkType {
        BOLT,
        S3
    }

    @Override
    public Map<String,String> handleRequest(Map<String,String> event, Context context) {

        RequestType requestType = RequestType.valueOf(event.get("requestType").toUpperCase());
        SdkType sdkType = SdkType.valueOf(event.get("sdkType").toUpperCase());

        S3Client s3 = null;
        if (sdkType == SdkType.BOLT) {
            s3 = BoltS3Client.builder().build();
        } else if (sdkType == SdkType.S3) {
            s3 = S3Client.builder().build();
        }

        Map<String,String> respMap;
        try {
            switch (requestType) {
                case GET_OBJECT:
                    respMap = getObject(s3, event.get("bucket"), event.get("key"));
                    break;
                case LIST_OBJECTS_V2:
                    respMap = listObjectV2(s3, event.get("bucket"));
                    break;
                case HEAD_OBJECT:
                    respMap = headObject(s3, event.get("bucket"), event.get("key"));
                    break;
                default:
                    respMap = new HashMap<>();
                    break;
            }
        } catch (S3Exception e) {
            String msg = e.awsErrorDetails().errorMessage();
            String code = e.awsErrorDetails().errorCode();
            System.err.println(msg);
            respMap = new HashMap<String, String>() {{
                put("errorMessage", msg);
                put("errorCode", code);
            }};
        } catch (SdkClientException e) {
            String msg = e.toString();
            System.err.println(msg);
            respMap = new HashMap<String, String>() {{
                put("errorMessage", msg);
            }};
        }
        return respMap;
    }

    private static Map<String, String> getObject(S3Client s3, String bucket, String key) throws SdkClientException,
            S3Exception {

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
        } catch (NoSuchAlgorithmException e) {
            String msg = e.toString();
            System.err.println(msg);
            Map<String,String> map = new HashMap<String, String>() {{
                put("errorMessage", msg);
            }};
            return map;
        }
    }

    private static Map<String, String> listObjectV2(S3Client s3, String bucket) throws SdkClientException, S3Exception {

        ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(bucket).build();
        ListObjectsV2Response resp;

        resp = s3.listObjectsV2(req);

        List<S3Object> objects = resp.contents();
        Map<String,String> map = new HashMap<String, String>() {{
            put("objects", objects.toString());
        }};
        return map;
    }

    private static Map<String, String> headObject(S3Client s3, String bucket, String key) throws SdkClientException,
            S3Exception {

        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();

        HeadObjectResponse res = s3.headObject(headObjectRequest);
        Map<String,String> map = new HashMap<String,String>() {{
            put( "Expiration", res.expiration());
            put( "lastModified", res.lastModified().toString() );
            put( "ContentLength", res.contentLength().toString() );
            put( "ETag", res.eTag() );
            put( "VersionId", res.versionId() );
            put( "StorageClass", res.storageClass().toString() );
        }};
        return map;
    }
}
