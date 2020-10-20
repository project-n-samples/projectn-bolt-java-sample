package com.projectn.bolt;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class BoltS3OpsClient {

    enum RequestType {
        HEAD_OBJECT,
        GET_OBJECT,
        LIST_OBJECTS_V2,
        VALIDATE_OBJECT_MD5
    }

    enum SdkType {
        BOLT,
        S3
    }

    private S3Client s3;
    private S3Client boltS3;

    public BoltS3OpsClient() {
        s3 = boltS3 = null;
    }

    public Map<String, String> processEvent(Map<String, String> event) throws SdkClientException,
            S3Exception {

        BoltS3OpsClient.RequestType requestType = RequestType.valueOf(event.get("requestType").toUpperCase());
        String sdkTypeStr = event.get("sdkType");
        BoltS3OpsClient.SdkType sdkType = (sdkTypeStr != null && !sdkTypeStr.isEmpty()) ?
                SdkType.valueOf(sdkTypeStr.toUpperCase()) : null;

        if (sdkType == null) {
            s3 = S3Client.builder().build();
            boltS3 = BoltS3Client.builder().build();
        } else if (sdkType == SdkType.BOLT) {
            s3 = BoltS3Client.builder().build();
        } else if (sdkType == SdkType.S3) {
            s3 = S3Client.builder().build();
        }

        Map<String,String> respMap;
        try {
            switch (requestType) {
                case GET_OBJECT:
                    respMap = getObject(event.get("bucket"), event.get("key"));
                    break;
                case LIST_OBJECTS_V2:
                    respMap = listObjectV2(event.get("bucket"));
                    break;
                case HEAD_OBJECT:
                    respMap = headObject(event.get("bucket"), event.get("key"));
                    break;
                case VALIDATE_OBJECT_MD5:
                    respMap = validateObjectMD5(event.get("bucket"), event.get("key"));
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

    private Map<String, String> getObject(String bucket, String key) throws SdkClientException,
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

    private Map<String, String> listObjectV2(String bucket) throws SdkClientException, S3Exception {

        ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(bucket).build();
        ListObjectsV2Response resp;

        resp = s3.listObjectsV2(req);

        List<S3Object> objects = resp.contents();
        Map<String,String> map = new HashMap<String, String>() {{
            put("objects", objects.toString());
        }};
        return map;
    }

    private Map<String, String> headObject(String bucket, String key) throws SdkClientException,
            S3Exception {

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

    private Map<String, String> validateObjectMD5(String bucket, String key) throws SdkClientException,
            S3Exception {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();

        try {
            // Get Object from Bolt.
            ResponseBytes<GetObjectResponse> boltS3ResponseBytes = boltS3.getObjectAsBytes(getObjectRequest);
            // Get object from S3.
            ResponseBytes<GetObjectResponse> s3ResponseBytes = s3.getObjectAsBytes(getObjectRequest);

            // Parse the MD5 of the returned object
            MessageDigest md = MessageDigest.getInstance("MD5");
            String s3Md5, boltS3Md5;

            // If Object is gzip encoded, compute MD5 on the decompressed object.
            String encoding = s3ResponseBytes.response().contentEncoding();
            if (encoding != null && encoding.equalsIgnoreCase("gzip")) {
                GZIPInputStream gis;
                ByteArrayOutputStream output;
                byte[] buffer = new byte[1024];
                int len;

                // MD5 of the S3 object after gzip decompression.
                gis = new GZIPInputStream(s3ResponseBytes.asInputStream());
                output = new ByteArrayOutputStream();

                while ((len = gis.read(buffer)) > 0) {
                    output.write(buffer, 0, len);
                }

                md.update(output.toByteArray());
                s3Md5 = DatatypeConverter.printHexBinary(md.digest()).toUpperCase();
                output.close();
                gis.close();

                // MD5 of the Bolt object after gzip decompression.
                gis = new GZIPInputStream(boltS3ResponseBytes.asInputStream());
                output = new ByteArrayOutputStream();

                while ((len = gis.read(buffer)) > 0) {
                    output.write(buffer, 0, len);
                }

                md.reset();
                md.update(output.toByteArray());
                boltS3Md5 = DatatypeConverter.printHexBinary(md.digest()).toUpperCase();
                output.close();
                gis.close();
            } else  {
                // MD5 of the S3 object
                md.update(s3ResponseBytes.asByteArray());
                s3Md5 = DatatypeConverter.printHexBinary(md.digest()).toUpperCase();

                //MD5 of the Bolt object
                md.reset();
                md.update(boltS3ResponseBytes.asByteArray());
                boltS3Md5 = DatatypeConverter.printHexBinary(md.digest()).toUpperCase();
            }

            Map<String,String> map = new HashMap<String, String>() {{
                put("s3-md5", s3Md5);
                put("bolt-md5", boltS3Md5);
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
        catch (IOException e) {
            String msg = e.toString();
            System.err.println(msg);
            Map<String,String> map = new HashMap<String, String>() {{
                put("errorMessage", msg);
            }};
            return map;
        }
    }
}
