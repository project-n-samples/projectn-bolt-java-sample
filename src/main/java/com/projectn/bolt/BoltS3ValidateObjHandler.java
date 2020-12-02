package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * BoltS3ValidateObjHandler is a handler class that encapsulates the handler function handleRequest, which performs
 * data validation tests and is called by AWS Lambda when the function is invoked.
 */
public class BoltS3ValidateObjHandler implements RequestHandler<Map<String,String>, Map<String,String>> {

    // Indicates if source bucket is cleaned post crunch.
    enum BucketClean {
        // bucket is cleaned post crunch
        ON,
        // bucket is not cleaned post crunch
        OFF
    }

    /**
     * handlerRequest is the handler function that is invoked by AWS Lambda to process an incoming event for
     * performing data validation tests.
     *
     * handleRequest accepts the following input parameters as part of the event:
     * 1) bucket - bucket name
     * 2) key - key name
     *
     * handleRequest retrieves the object from Bolt and S3 (if BucketClean is OFF), computes and returns their
     * corresponding MD5 hash. If the object is gzip encoded, object is decompressed before computing its MD5.
     * @param event incoming event object
     * @param context Lambda execution environment context object
     * @return md5s of object retrieved from Bolt and S3.
     */
    @Override
    public Map<String,String> handleRequest(Map<String,String> event, Context context) {

        String bucket = event.get("bucket");
        String key = event.get("key");
        String bucketCleanStr = event.get("bucketClean");
        BucketClean bucketClean = (bucketCleanStr != null && !bucketCleanStr.isEmpty()) ?
                BucketClean.valueOf(bucketCleanStr.toUpperCase()) : BucketClean.OFF;

        S3Client s3 = S3Client.builder().build();
        S3Client boltS3 = BoltS3Client.builder().build();

        Map<String,String> respMap;

        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();

            // Get Object from Bolt.
            ResponseBytes<GetObjectResponse> boltS3ResponseBytes = boltS3.getObjectAsBytes(getObjectRequest);
            // Get Object from S3 if bucket clean is off.
            ResponseBytes<GetObjectResponse> s3ResponseBytes = null;
            if (bucketClean == BucketClean.OFF) {
                s3ResponseBytes = s3.getObjectAsBytes(getObjectRequest);
            }

            // Parse the MD5 of the returned object
            MessageDigest md = MessageDigest.getInstance("MD5");
            String s3Md5, boltS3Md5;

            // If Object is gzip encoded, compute MD5 on the decompressed object.
            String encoding = s3ResponseBytes.response().contentEncoding();
            if ((encoding != null && encoding.equalsIgnoreCase("gzip")) ||
                    key.endsWith(".gz")) {
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
}
