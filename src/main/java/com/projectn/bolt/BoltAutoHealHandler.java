package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.HashMap;
import java.util.Map;

/**
 * BoltAutoHealHandler is a handler class that encapsulates the handler function HandleRequest, which performs
 * auto-heal tests and is called by AWS Lambda when the function is invoked.
 */
public class BoltAutoHealHandler implements RequestHandler<Map<String,String>, Map<String,String>> {

    /**
     * handleRequest is the handler function that is invoked by AWS Lambda to process an incoming event for
     * performing auto-heal tests.
     *
     * lambda_handler accepts the following input parameters as part of the event:
     * 1) bucket - bucket name
     * 2) key - key name
     *
     * @param event incoming event object
     * @param context Lambda execution environment context object
     * @return time taken to auto-heal
     */
    @Override
    public Map<String,String> handleRequest(Map<String,String> event, Context context) {

        String bucket = event.get("bucket");
        String key = event.get("key");

        // Bolt client.
        S3Client boltS3 = BoltS3Client.builder().build();

        // Attempt to retrieve object repeatedly until it succeeds, which would indicate successful
        // auto-healing of the object.
        long autoHealEndTime;
        long autoHealStartTime = System.currentTimeMillis();
        while (true) {
            try {
                // Get object from Bolt.
                GetObjectRequest getObjectRequest =
                        GetObjectRequest
                                .builder()
                                .bucket(bucket)
                                .key(key)
                                .build();

                ResponseInputStream<GetObjectResponse> resp =
                        boltS3.getObject(getObjectRequest);

                /*
                // read all data from the stream.
                byte[] readBuffer = new byte[4096];
                while (resp.read(readBuffer, 0, readBuffer.length) != -1);
                */

                autoHealEndTime = System.currentTimeMillis();
                // close response stream.
                resp.close();
                // exit on success after auto-heal
                break;
            } catch (S3Exception e) {
                // Ignore S3Exception
            } catch (Exception e) {
                // Ignore Exception
            }
        }
        long autoHealTime = autoHealEndTime - autoHealStartTime;
        return new HashMap<String, String>(){{
           put("auto_heal_time", String.format("%d ms", autoHealTime));
        }};
    }
}
