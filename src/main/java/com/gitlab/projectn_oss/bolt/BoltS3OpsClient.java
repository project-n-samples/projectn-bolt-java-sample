package com.gitlab.projectn_oss.bolt;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * BoltS3OpsClient processes AWS Lambda events that are received by the handler function
 * BoltS3OpsHandler.handleRequest.
 */
public class BoltS3OpsClient {

    // request types supported
    enum RequestType {
        HEAD_OBJECT,
        GET_OBJECT,
        LIST_OBJECTS_V2,
        LIST_BUCKETS,
        HEAD_BUCKET,
        PUT_OBJECT,
        DELETE_OBJECT
    }

    // endpoints supported
    enum SdkType {
        BOLT,
        S3
    }

    private S3Client s3;

    public BoltS3OpsClient() {
        s3 = null;
    }

    /**
     * processEvent extracts the parameters (sdkType, requestType, bucket/key) from the event, uses those
     * parameters to send an Object/Bucket CRUD request to Bolt/S3 and returns back an appropriate response.
     * @param event incoming Lambda event object
     * @return result of the requested operation returned by the endpoint (sdkType)
     */
    public Map<String, String> processEvent(Map<String, String> event) {

        BoltS3OpsClient.RequestType requestType = RequestType.valueOf(event.get("requestType").toUpperCase());
        String sdkTypeStr = event.get("sdkType");
        BoltS3OpsClient.SdkType sdkType = (sdkTypeStr != null && !sdkTypeStr.isEmpty()) ?
                SdkType.valueOf(sdkTypeStr.toUpperCase()) : null;

        // create an S3/Bolt Client depending on the 'sdkType'
        // If sdkType is not specified, create an S3 Client.
        if (sdkType == null || sdkType == SdkType.S3) {
            s3 = S3Client.builder().build();
        } else if (sdkType == SdkType.BOLT) {
            s3 = BoltS3Client.builder().build();
        }

        // Perform an S3 / Bolt operation based on the input 'requestType'
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
                case PUT_OBJECT:
                    respMap = putObject(event.get("bucket"), event.get("key"), event.get("value"));
                    break;
                case DELETE_OBJECT:
                    respMap = deleteObject(event.get("bucket"), event.get("key"));
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

    /**
     * Gets the object from Bolt/S3, computes and returns the object's MD5 hash. If the object is gzip encoded, object
     * is decompressed before computing its MD5.
     * @param bucket bucket name
     * @param key key name
     * @return md5 hash of the object
     * @throws Exception
     */
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

    /**
     * Returns a list of 1000 objects from the given bucket in Bolt/S3
     * @param bucket bucket name
     * @return list of first 1000 objects
     * @throws Exception
     */
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

    /**
     * Retrieves the object's metadata from Bolt / S3.
     * @param bucket bucket name
     * @param key key name
     * @return object metadata
     * @throws Exception
     */
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

    /**
     * Returns list of buckets owned by the sender of the request
     * @return list of buckets
     * @throws Exception
     */
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

    /**
     * Checks if the bucket exists in Bolt/S3.
     * @param bucket bucket name
     * @return status code and Region if the bucket exists
     * @throws Exception
     */
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

    /**
     * Uploads an object to Bolt/S3.
     * @param bucket bucket name
     * @param key key name
     * @param value object data
     * @return metadata of object
     * @throws Exception
     */
    private Map<String, String> putObject(String bucket, String key, String value) throws Exception {

        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucket).key(key).build();

        PutObjectResponse res = s3.putObject(putObjectRequest, RequestBody.fromString(value));
        Map<String,String> map = new HashMap<String,String>() {{
           put("ETag", res.eTag());
           put( "Expiration", res.expiration());
           put( "VersionId", res.versionId());
        }};
        return map;
    }

    /**
     * Delete an object from Bolt/S3
     * @param bucket bucket name
     * @param key key name
     * @return status code
     * @throws Exception
     */
    private Map<String, String> deleteObject(String bucket, String key) throws Exception {

        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build();

        DeleteObjectResponse res = s3.deleteObject(deleteObjectRequest);
        Map<String,String> map = new HashMap<String,String>() {{
            put("statusCode", String.valueOf(res.sdkHttpResponse().statusCode()));
            put("statusText", res.sdkHttpResponse().statusText().orElse(""));
        }};
        return map;
    }
}
