package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.util.Map;

/**
 * BoltS3PerfHandler is a handler class that encapsulates the handler function handleRequest, which performs
 * Bolt/S3 Performance tests and is called by AWS Lambda when the function is invoked.
 */
public class BoltS3PerfHandler implements RequestHandler<Map<String,String>, Map<String,Map<String, Map<String, String>>>> {

    /**
     * handleRequest is the handler function that is invoked by AWS Lambda to process an incoming event for
     * Bolt / S3 Performance testing.
     *
     * handleRequest accepts the following input parameters as part of the event:
     * 1) requestType - type of request / operation to be performed.The following requests are supported:
     *    a) list_objects_v2 - list objects
     *    b) get_object - get object
     *    c) get_object_ttfb - get object (first byte)
     *    d) get_object_passthrough - get object (via passthrough) of unmonitored bucket
     *    e) get_object_passthrough_ttfb - get object (first byte via passthrough) of unmonitored bucket
     *    f) put_object - upload object
     *    g) delete_object - delete object
     *    h) all - put, get, delete, list objects(default request if none specified)
     *
     * 2) bucket - bucket name
     *
     * Following are examples of events, for various requests, that can be used to invoke the handler function.
     * a) Measure List objects performance of Bolt/S3.
     *    {"requestType": "list_objects_v2", "bucket": "<bucket>"}
     *
     * b) Measure Get object performance of Bolt / S3.
     *    {"requestType": "get_object", "bucket": "<bucket>"}
     *
     * c) Measure Get object (first byte) performance of Bolt / S3.
     *    {"requestType": "get_object_ttfb", "bucket": "<bucket>"}
     *
     * d) Measure Get object passthrough performance of Bolt.
     *    {"requestType": "get_object_passthrough", "bucket": "<unmonitored-bucket>"}
     *
     * e) Measure Get object passthrough (first byte) performance of Bolt.
     *    {"requestType": "get_object_passthrough_ttfb", "bucket": "<unmonitored-bucket>"}
     *
     * f) Measure Put object performance of Bolt / S3.
     *    {"requestType": "put_object", "bucket": "<bucket>"}
     *
     * g) Measure Delete object performance of Bolt / S3.
     *    {"requestType": "delete_object", "bucket": "<bucket>"}
     *
     * h) Measure Put, Delete, Get, List objects performance of Bolt / S3.
     *    {"requestType": "all", "bucket": "<bucket>"}
     *
     * @param event incoming event object
     * @param context Lambda execution environment context object
     * @return response from BoltS3Perf
     */
    @Override
    public Map<String,Map<String, Map<String, String>>> handleRequest(Map<String,String> event, Context context) {

        BoltS3Perf boltS3Perf = new BoltS3Perf();
        return boltS3Perf.processEvent(event);
    }
}
