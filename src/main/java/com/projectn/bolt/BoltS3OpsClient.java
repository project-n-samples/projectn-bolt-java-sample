package com.projectn.bolt;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class BoltS3OpsClient {

    enum RequestType {
        HEAD_OBJECT,
        GET_OBJECT,
        LIST_OBJECTS_V2,
        LIST_BUCKETS,
        HEAD_BUCKET
    }

    enum SdkType {
        BOLT,
        S3
    }

    private S3Client s3;

    public BoltS3OpsClient() {
        s3 = null;
    }

    public Map<String, String> processEvent(Map<String, String> event) {

        BoltS3OpsClient.RequestType requestType = RequestType.valueOf(event.get("requestType").toUpperCase());
        String sdkTypeStr = event.get("sdkType");
        BoltS3OpsClient.SdkType sdkType = (sdkTypeStr != null && !sdkTypeStr.isEmpty()) ?
                SdkType.valueOf(sdkTypeStr.toUpperCase()) : null;

        // If sdkType is not specified, create an S3 Client.
        if (sdkType == null || sdkType == SdkType.S3) {
            s3 = S3Client.builder().build();
        } else if (sdkType == SdkType.BOLT) {
            s3 = BoltS3Client.builder().build();
        }

        Map<String,String> respMap;
        try {
            switch (requestType) {
                case GET_OBJECT:
                    respMap = getObject(event.get("bucket"), event.get("key"));
                    break;
                case LIST_OBJECTS_V2:
                    respMap = listObjectsV2(event.get("bucket"));
                    break;
                case HEAD_OBJECT:
                    respMap = headObject(event.get("bucket"), event.get("key"));
                    break;
                case LIST_BUCKETS:
                    respMap = listBuckets();
                    break;
                case HEAD_BUCKET:
                    respMap = headBucket(event.get("bucket"));
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
        } catch (Exception e) {
            String msg = e.toString();
            System.err.println(msg);
            respMap = new HashMap<String, String>() {{
                put("errorMessage", msg);
            }};
        }
        return respMap;
    }

    private Map<String, String> getObject(String bucket, String key) throws Exception {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();

        // Get Object.
        ResponseBytes<GetObjectResponse> respBytes = s3.getObjectAsBytes(getObjectRequest);

        // Parse the MD5 of the returned object
        MessageDigest md = MessageDigest.getInstance("MD5");
        String md5;

        // If Object is gzip encoded, compute MD5 on the decompressed object.
        String encoding = respBytes.response().contentEncoding();
        System.out.println("Encoding:" + encoding);
        if ((encoding != null && encoding.equalsIgnoreCase("gzip")) ||
                key.endsWith(".gz")) {

            // MD5 of the object after gzip decompression.
            GZIPInputStream gis = new GZIPInputStream(respBytes.asInputStream());
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len;

            while ((len = gis.read(buffer)) > 0) {
                output.write(buffer, 0, len);
            }

            md.update(output.toByteArray());
            md5 = DatatypeConverter.printHexBinary(md.digest()).toUpperCase();
            output.close();
            gis.close();
        } else {
            md.update(respBytes.asByteArray());
            md5 = DatatypeConverter.printHexBinary(md.digest()).toUpperCase();
        }

        Map<String,String> map = new HashMap<String, String>() {{
            put("md5", md5);
        }};
        return map;
    }

    private Map<String, String> listObjectsV2(String bucket) throws Exception {

        ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(bucket).build();
        ListObjectsV2Response resp;

        resp = s3.listObjectsV2(req);

        List<S3Object> objects = resp.contents();
        Map<String,String> map = new HashMap<String, String>() {{
            put("objects", objects.toString());
        }};
        return map;
    }

    private Map<String, String> headObject(String bucket, String key) throws Exception {

        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();

        HeadObjectResponse res = s3.headObject(headObjectRequest);
        Map<String,String> map = new HashMap<String,String>() {{
            put( "Expiration", res.expiration());
            put( "lastModified", res.lastModified().toString() );
            put( "ContentLength", res.contentLength().toString() );
            put( "ContentEncoding", res.contentEncoding() );
            put( "ETag", res.eTag() );
            put( "VersionId", res.versionId() );
            put( "StorageClass", res.storageClass() != null ? res.storageClass().toString() : "" );
        }};
        return map;
    }

    private Map<String, String> listBuckets() throws Exception {

        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse resp;

        resp = s3.listBuckets(listBucketsRequest);

        List<Bucket> buckets = resp.buckets();
        Map<String,String> map = new HashMap<String, String>() {{
            put("buckets", buckets.toString());
        }};
        return map;
    }

    private Map<String, String> headBucket(String bucket) throws Exception {

        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(bucket).build();

        HeadBucketResponse res = s3.headBucket(headBucketRequest);
        Map<String, List<String>> resHeaders = res.sdkHttpResponse().headers();
        String bucketRegion = resHeaders.get("x-amz-bucket-region") != null ?
                resHeaders.get("x-amz-bucket-region").get(0) : "";
        Map<String,String> map = new HashMap<String, String>() {{
            put("statusCode", String.valueOf(res.sdkHttpResponse().statusCode()));
            put("statusText", res.sdkHttpResponse().statusText().orElse(""));
            put("region", bucketRegion);
        }};
        return map;
    }
}
