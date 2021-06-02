package com.projectn.bolt;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BoltS3Perf processes AWS Lambda events that are received by the handler function
 * BoltS3PerfHandler.HandleRequest for Bolt / S3 Performance testing.
 */
public class BoltS3Perf {

    // request types supported
    enum RequestType {
        LIST_OBJECTS_V2,
        PUT_OBJECT,
        DELETE_OBJECT,
        GET_OBJECT,
        GET_OBJECT_TTFB,
        GET_OBJECT_PASSTHROUGH,
        GET_OBJECT_PASSTHROUGH_TTFB,
        ALL
    }

    // Bolt and S3 Clients.
    private S3Client s3;
    private S3Client boltS3;

    // max. number of keys to be used in Perf.
    private int numKeys;
    // length of object data.
    private int objLength;
    // type of perf to be performed.
    private RequestType requestType;

    // list of keys for Perf tests.
    private List<String> keys;

    public BoltS3Perf() {
        s3 = S3Client.builder().build();
        boltS3 = BoltS3Client.builder().build();
    }

    /**
     * process_event extracts the parameters (requestType, bucket) from the event, uses those
     * parameters to run performance testing against Bolt / S3 and returns back performance statistics.
     * @param event incoming Lambda event object
     * @return performance statistics.
     */
    public Map<String, Map<String, Map<String, String>>> processEvent(Map<String, String> event) {

        // If requestType is not passed, perform all perf tests.
        String requestTypeStr = event.get("requestType");
        requestType = (requestTypeStr != null && !requestTypeStr.isEmpty()) ?
                RequestType.valueOf(requestTypeStr.toUpperCase()) : RequestType.ALL;

        // update max no of keys and object data length, in passed in as input.
        String numKeysStr = event.get("numKeys");
        numKeys = (numKeysStr != null && !numKeysStr.isEmpty()) ?
                Integer.parseInt(numKeysStr) : 1000;
        if (numKeys > 1000) {
            numKeys = 1000;
        }

        String objLengthStr = event.get("objLength");
        objLength = (objLengthStr != null && !objLengthStr.isEmpty()) ?
                Integer.parseInt(objLengthStr) : 100;

        HashMap<String, Map<String, Map<String, String>>> respMap = null;
        try {

            // If Put, Delete, All Object then generate key names
            // If Get Object (including passthrough), list objects (up to numKeys) to get key names.
            if (requestType == RequestType.PUT_OBJECT ||
                    requestType == RequestType.DELETE_OBJECT ||
                    requestType == RequestType.ALL) {
                keys = generateKeyNames(numKeys);
            } else if (requestType == RequestType.GET_OBJECT ||
                    requestType == RequestType.GET_OBJECT_PASSTHROUGH ||
                    requestType == RequestType.GET_OBJECT_TTFB ||
                    requestType == RequestType.GET_OBJECT_PASSTHROUGH_TTFB) {
                keys = listObjectsV2(event.get("bucket"));
            }

            switch (requestType) {
                case LIST_OBJECTS_V2:
                    respMap = listObjectsV2Perf(event.get("bucket"));
                    break;
                case PUT_OBJECT:
                    respMap = putObjectPerf(event.get("bucket"));
                    break;
                case DELETE_OBJECT:
                    respMap = deleteObjectPerf(event.get("bucket"));
                    break;
                case GET_OBJECT:
                case GET_OBJECT_TTFB:
                    respMap = getObjectPerf(event.get("bucket"));
                    break;
                case GET_OBJECT_PASSTHROUGH:
                case GET_OBJECT_PASSTHROUGH_TTFB:
                    respMap = getObjectPassthroughPerf(event.get("bucket"));
                    break;
                case ALL:
                    respMap = allPerf(event.get("bucket"));
                    break;
                default:
                    respMap = new HashMap<>();
                    break;
            }
        } catch (S3Exception e) {
            String msg = e.awsErrorDetails().errorMessage();
            String code = e.awsErrorDetails().errorCode();

            Map<String, String> errorMap = new HashMap<String, String>() {{
                put("errorMessage", msg);
                put("errorCode", code);
            }};

            Map<String, Map<String, String>> s3ExceptionMap =
                    new HashMap<String, Map<String, String>>() {{
                put("AmazonS3Exception", errorMap);
            }};

            respMap = new HashMap<String, Map<String, Map<String, String>>>() {{
               put("error", s3ExceptionMap);
            }};

        } catch (Exception e) {
            String msg = e.toString();

            Map<String, String> errorMap = new HashMap<String, String>() {{
                put("errorMessage", msg);
            }};

            Map<String, Map<String, String>> exceptionMap =
                    new HashMap<String, Map<String, String>>() {{
                        put("Exception", errorMap);
                    }};

            respMap = new HashMap<String, Map<String, Map<String, String>>>() {{
                put("error", exceptionMap);
            }};
        }
        return respMap;
    }

    /**
     * Measures the List Objects V2 performance (latency, throughput) of Bolt / S3.
     * @param bucket bucket name
     * @return List Objects v2 performance statistics
     * @throws Exception
     */
    private HashMap<String, Map<String, Map<String, String>>> listObjectsV2Perf(String bucket) throws Exception {
        List<Long> s3ListObjTimes = new ArrayList<>();
        List<Long> boltListObjTimes = new ArrayList<>();
        List<Double> s3ListObjTp = new ArrayList<>();
        List<Double> boltListObjTp = new ArrayList<>();

        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucket)
                .maxKeys(1000)
                .build();

        // list 1000 objects from S3, 10 times.
        for (int i = 1; i <= 10 ; i++) {
            long listObjStartTime = System.currentTimeMillis();

            ListObjectsV2Response resp;
            resp = s3.listObjectsV2(req);
            long listObjEndTime = System.currentTimeMillis();

            // calc latency
            long listObjV2Time = listObjEndTime - listObjStartTime;
            s3ListObjTimes.add(listObjV2Time);

            // calc throughput
            double listObjV2Tp = resp.keyCount().doubleValue() / listObjV2Time;
            s3ListObjTp.add(listObjV2Tp);
        }

        // list 1000 objects from Bolt, 10 times.
        for (int i = 1; i <= 10 ; i++) {
            long listObjStartTime = System.currentTimeMillis();

            ListObjectsV2Response resp;
            resp = boltS3.listObjectsV2(req);
            long listObjEndTime = System.currentTimeMillis();

            // calc latency
            long listObjV2Time = listObjEndTime - listObjStartTime;
            boltListObjTimes.add(listObjV2Time);

            // calc throughput
            double listObjV2Tp = resp.keyCount().doubleValue() / listObjV2Time;
            boltListObjTp.add(listObjV2Tp);
        }

        // calc s3 perf stats.
        Map<String, Map<String, String>> s3ListObjPerfStats = computePerfStats(s3ListObjTimes, s3ListObjTp,
                null);

        // calc bolt perf stats.
        Map<String, Map<String, String>> boltListObjPerfStats = computePerfStats(boltListObjTimes, boltListObjTp,
                null);

        return new HashMap<String, Map<String, Map<String, String>>>() {{
           put("s3_list_objects_v2_perf_stats", s3ListObjPerfStats);
           put("bolt_list_objects_v2_perf_stats", boltListObjPerfStats);
        }};
    }

    /**
     * Measures the Put Object performance (latency, throughput) of Bolt / S3.
     * @param bucket bucket name
     * @return Put object performance statistics
     * @throws Exception
     */
    private HashMap<String, Map<String, Map<String, String>>> putObjectPerf(String bucket) throws Exception {
        List<Long> s3PutObjTimes = new ArrayList<>();
        List<Long> boltPutObjTimes = new ArrayList<>();

        // Upload objects to Bolt / S3.
        for (String key: keys) {
            String value = generate(objLength);

            PutObjectRequest putObjectRequest = PutObjectRequest
                    .builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            // upload object to S3.
            long putObjStartTime = System.currentTimeMillis();
            PutObjectResponse resp = s3.putObject(putObjectRequest, RequestBody.fromString(value));
            long putObjEndTime = System.currentTimeMillis();

            // calc latency
            long putObjTime = putObjEndTime - putObjStartTime;
            s3PutObjTimes.add(putObjTime);

            // upload object to Bolt.
            putObjStartTime = System.currentTimeMillis();
            resp = boltS3.putObject(putObjectRequest, RequestBody.fromString(value));
            putObjEndTime = System.currentTimeMillis();

            // calc latency
            putObjTime = putObjEndTime - putObjStartTime;
            boltPutObjTimes.add(putObjTime);
        }

        // calc s3 perf stats.
        Map<String, Map<String, String>> s3PutObjPerfStats = computePerfStats(s3PutObjTimes, null,
                null);

        // calc bolt perf stats.
        Map<String, Map<String, String>> boltPutObjPerfStats = computePerfStats(boltPutObjTimes, null,
                null);

        return new HashMap<String, Map<String, Map<String, String>>>() {{
            put("s3_put_obj_perf_stats", s3PutObjPerfStats);
            put("bolt_put_obj_perf_stats", boltPutObjPerfStats);
        }};
    }

    /**
     * Measures the Delete Object performance (latency, throughput) of Bolt/S3.
     * @param bucket bucket name
     * @return Delete Object performance statistics.
     * @throws Exception
     */
    private HashMap<String, Map<String, Map<String, String>>> deleteObjectPerf(String bucket) throws Exception {
        List<Long> s3DelObjTimes = new ArrayList<>();
        List<Long> boltDelObjTimes = new ArrayList<>();

        // Delete objects from Bolt / S3.
        for (String key: keys) {
            DeleteObjectRequest deleteObjectRequest =
                    DeleteObjectRequest
                            .builder()
                            .bucket(bucket)
                            .key(key)
                            .build();

            // Delete object from S3.
            long delObjStartTime = System.currentTimeMillis();
            DeleteObjectResponse resp = s3.deleteObject(deleteObjectRequest);
            long delObjEndTime = System.currentTimeMillis();

            // calc latency
            long delObjTime = delObjEndTime - delObjStartTime;
            s3DelObjTimes.add(delObjTime);

            // Delete object from Bolt.
            delObjStartTime = System.currentTimeMillis();
            resp = boltS3.deleteObject(deleteObjectRequest);
            delObjEndTime = System.currentTimeMillis();

            // calc latency
            delObjTime = delObjEndTime - delObjStartTime;
            boltDelObjTimes.add(delObjTime);
        }

        // calc s3 perf stats.
        Map<String, Map<String, String>> s3DelObjPerfStats = computePerfStats(s3DelObjTimes, null,
                null);

        // calc bolt perf stats.
        Map<String, Map<String, String>> boltDelObjPerfStats = computePerfStats(boltDelObjTimes, null,
                null);

        return new HashMap<String, Map<String, Map<String, String>>>() {{
            put("s3_del_obj_perf_stats", s3DelObjPerfStats);
            put("bolt_del_obj_perf_stats", boltDelObjPerfStats);
        }};
    }

    /**
     * Measures the Get Object performance (latency, throughput) of Bolt / S3.
     * @param bucket bucket name
     * @return Get Object performance statistics
     * @throws Exception
     */
    private HashMap<String, Map<String, Map<String, String>>> getObjectPerf(String bucket) throws Exception {
        List<Long> s3GetObjTimes = new ArrayList<>();
        List<Long> boltGetObjTimes = new ArrayList<>();

        List<Long> s3ObjSizes = new ArrayList<>();
        List<Long> boltObjSizes = new ArrayList<>();

        int s3CmpObjCount = 0;
        int s3UnCmpObjCount = 0;
        int boltCmpObjCount = 0;
        int boltUnCmpObjCount = 0;

        // Get Objects from S3.
        for (String key: keys) {
            GetObjectRequest getObjectRequest =
                    GetObjectRequest
                            .builder()
                            .bucket(bucket)
                            .key(key)
                            .build();

            long getObjStartTime = System.currentTimeMillis();
            ResponseInputStream<GetObjectResponse> resp =
                    s3.getObject(getObjectRequest);
            // If getting first byte object latency, read at most 1 byte,
            // otherwise read the entire body.
            if (requestType == RequestType.GET_OBJECT_TTFB) {
                // read only first byte from the stream.
                resp.read();
            } else {
                // read all data from the stream.
                byte[] readBuffer = new byte[4096];
                while (resp.read(readBuffer, 0, readBuffer.length) != -1);
            }
            long getObjEndTime = System.currentTimeMillis();

            // calc latency
            long getObjTime = getObjEndTime - getObjStartTime;
            s3GetObjTimes.add(getObjTime);

            // count object
            String encoding = resp.response().contentEncoding();
            if ((encoding != null && encoding.equalsIgnoreCase("gzip")) ||
                    key.endsWith(".gz")) {
                s3CmpObjCount++;
            } else {
                s3UnCmpObjCount++;
            }

            // get object size.
            s3ObjSizes.add(resp.response().contentLength());
            // close response stream.
            resp.close();
        }

        // Get Objects from Bolt.
        for (String key: keys) {
            GetObjectRequest getObjectRequest =
                    GetObjectRequest
                            .builder()
                            .bucket(bucket)
                            .key(key)
                            .build();

            long getObjStartTime = System.currentTimeMillis();
            ResponseInputStream<GetObjectResponse> resp =
                    boltS3.getObject(getObjectRequest);
            // If getting first byte object latency, read at most 1 byte,
            // otherwise read the entire body.
            if (requestType == RequestType.GET_OBJECT_TTFB) {
                // read only first byte from the stream.
                resp.read();
            } else {
                // read all data from the stream.
                byte[] readBuffer = new byte[4096];
                while (resp.read(readBuffer, 0, readBuffer.length) != -1);
            }
            long getObjEndTime = System.currentTimeMillis();

            // calc latency
            long getObjTime = getObjEndTime - getObjStartTime;
            boltGetObjTimes.add(getObjTime);

            // count object
            String encoding = resp.response().contentEncoding();
            if ((encoding != null && encoding.equalsIgnoreCase("gzip")) ||
                    key.endsWith(".gz")) {
                boltCmpObjCount++;
            } else {
                boltUnCmpObjCount++;
            }

            // get object size.
            boltObjSizes.add(resp.response().contentLength());
            // close response stream.
            resp.close();
        }

        // calc s3 perf stats.
        Map<String, Map<String, String>> s3GetObjPerfStats = computePerfStats(s3GetObjTimes, null,
                s3ObjSizes);

        // calc bolt perf stats.
        Map<String, Map<String, String>> boltGetObjPerfStats = computePerfStats(boltGetObjTimes, null,
                boltObjSizes);

        String s3GetObjStatName, boltGetObjStatName;
        if (requestType == RequestType.GET_OBJECT_TTFB) {
            s3GetObjStatName = "s3_get_obj_ttfb_perf_stats";
            boltGetObjStatName = "bolt_get_obj_ttfb_perf_stats";
        } else {
            s3GetObjStatName = "s3_get_obj_perf_stats";
            boltGetObjStatName = "bolt_get_obj_perf_stats";
        }

        Map<String, String> s3Count = new HashMap<>();
        s3Count.put("compressed", String.valueOf(s3CmpObjCount));
        s3Count.put("uncompressed", String.valueOf(s3UnCmpObjCount));

        Map<String, String> boltCount = new HashMap<>();
        boltCount.put("compressed", String.valueOf(boltCmpObjCount));
        boltCount.put("uncompressed", String.valueOf(boltUnCmpObjCount));

        Map<String, Map<String, String>> objCount = new HashMap<String, Map<String, String>>() {{
            put("s3Count", s3Count);
            put("boltCount", boltCount);
        }};

        return new HashMap<String, Map<String, Map<String, String>>>() {{
            put(s3GetObjStatName, s3GetObjPerfStats);
            put(boltGetObjStatName, boltGetObjPerfStats);
            put("object_count", objCount);
        }};
    }

    /**
     * Measures the Get Object passthrough performance (latency, throughput) of Bolt / S3.
     * @param bucket bucket name
     * @return Get Object passthrough performance statistics.
     * @throws Exception
     */
    private HashMap<String, Map<String, Map<String, String>>> getObjectPassthroughPerf(String bucket) throws Exception {
        List<Long> boltGetObjTimes = new ArrayList<>();

        List<Long> boltObjSizes = new ArrayList<>();

        int boltCmpObjCount = 0;
        int boltUnCmpObjCount = 0;

        // Get Objects via passthrough from Bolt.
        for (String key: keys) {
            GetObjectRequest getObjectRequest =
                    GetObjectRequest
                            .builder()
                            .bucket(bucket)
                            .key(key)
                            .build();

            long getObjStartTime = System.currentTimeMillis();
            ResponseInputStream<GetObjectResponse> resp =
                    boltS3.getObject(getObjectRequest);
            // If getting first byte object latency, read at most 1 byte,
            // otherwise read the entire body.
            if (requestType == RequestType.GET_OBJECT_PASSTHROUGH_TTFB) {
                // read only first byte from the stream.
                resp.read();
            } else {
                // read all data from the stream.
                byte[] readBuffer = new byte[4096];
                while (resp.read(readBuffer, 0, readBuffer.length) != -1);
            }
            long getObjEndTime = System.currentTimeMillis();

            // calc latency
            long getObjTime = getObjEndTime - getObjStartTime;
            boltGetObjTimes.add(getObjTime);

            // count object
            String encoding = resp.response().contentEncoding();
            if ((encoding != null && encoding.equalsIgnoreCase("gzip")) ||
                    key.endsWith(".gz")) {
                boltCmpObjCount++;
            } else {
                boltUnCmpObjCount++;
            }

            // get object size.
            boltObjSizes.add(resp.response().contentLength());
            // close response stream.
            resp.close();
        }

        // calc bolt perf stats.
        Map<String, Map<String, String>> boltGetObjPtPerfStats = computePerfStats(boltGetObjTimes, null,
                boltObjSizes);

        String boltGetObjPtStatName;
        if (requestType == RequestType.GET_OBJECT_PASSTHROUGH_TTFB) {
            boltGetObjPtStatName = "bolt_get_obj_pt_ttfb_perf_stats";
        } else {
            boltGetObjPtStatName = "bolt_get_obj_pt_perf_stats";
        }

        Map<String, String> boltCount = new HashMap<>();
        boltCount.put("compressed", String.valueOf(boltCmpObjCount));
        boltCount.put("uncompressed", String.valueOf(boltUnCmpObjCount));

        Map<String, Map<String, String>> objCount = new HashMap<String, Map<String, String>>() {{
            put("boltCount", boltCount);
        }};

        return new HashMap<String, Map<String, Map<String, String>>>() {{
            put(boltGetObjPtStatName, boltGetObjPtPerfStats);
            put("object_count", objCount);
        }};
    }

    /**
     * Measures PUT,GET,DELETE,List Objects performance (latency, throughput) of Bolt / S3.
     * @param bucket bucket name
     * @return Object Performance statistics
     * @throws Exception
     */
    private HashMap<String, Map<String, Map<String, String>>> allPerf(String bucket) throws Exception {
        // Put, Delete Object Perf tests using generated key names.
        HashMap<String, Map<String, Map<String, String>>> putObjPerfStats = putObjectPerf(bucket);
        HashMap<String, Map<String, Map<String, String>>> delObjPerfStats = deleteObjectPerf(bucket);

        // List Objects perf tests on existing objects.
        HashMap<String, Map<String, Map<String, String>>> listObjPerfStats = listObjectsV2Perf(bucket);

        // Get the list of objects before get object perf test.
        keys = listObjectsV2(bucket);
        HashMap<String, Map<String, Map<String, String>>> getObjPerfStats = getObjectPerf(bucket);

        HashMap<String, Map<String, Map<String, String>>> mergedPerfStats =
                new HashMap<>();
        mergedPerfStats.putAll(putObjPerfStats);
        mergedPerfStats.putAll(delObjPerfStats);
        mergedPerfStats.putAll(listObjPerfStats);
        mergedPerfStats.putAll(getObjPerfStats);

        return mergedPerfStats;
    }

    /**
     * Compute Performance Statistics
     * @param opTimes list of latencies
     * @param opTp list of throughputs
     * @param objSizes list of object sizes
     * @return performance statistics (latency, throughput, object size)
     */
    private Map<String, Map<String, String>> computePerfStats(List<Long> opTimes,
                                                              List<Double> opTp,
                                                              List<Long> objSizes) {

        // calc op latency perf
        double opAvgTime = opTimes.stream().mapToLong(l -> l).average().orElse(0.0);
        opTimes.sort(null);
        long opTimeP50 = opTimes.get(opTimes.size() / 2);
        int p90Index = (int)(opTimes.size() * 0.9);
        long opTimeP90 = opTimes.get(p90Index);

        Map<String, String> latencyPerfStats = new HashMap<String, String>(){{
           put("average", String.format("%.2f ms", opAvgTime));
           put("p50", String.format("%d ms", opTimeP50));
           put("p90", String.format("%d ms", opTimeP90));
        }};

        // calc op throughput perf.
        Map<String, String> tpPerfStats;
        if (opTp != null) {
            double opAvgTp = opTp.stream().mapToDouble(d -> d).average().orElse(0.0);
            opTp.sort(null);
            double opTpP50 = opTp.get(opTp.size() / 2);
            p90Index = (int)(opTp.size() * 0.9);
            double opTpP90 = opTp.get(p90Index);

            tpPerfStats = new HashMap<String, String>() {{
               put("average", String.format("%.2f objects/ms", opAvgTp));
               put("p50", String.format("%.2f objects/ms", opTpP50));
               put("p90", String.format("%.2f objects/ms", opTpP90));
            }};
        } else {
            double tp = (double) opTimes.size() / opTimes.stream().mapToLong(Long::longValue).sum();
            tpPerfStats = new HashMap<String, String>() {{
               put("throughput", String.format("%.2f objects/ms", tp));
            }};
        }

        // calc obj size metrics.
        Map<String, String> objSizesPerfStats = null;
        if (objSizes != null) {
            double obAvgSize = objSizes.stream().mapToLong(l -> l).average().orElse(0.0);
            objSizes.sort(null);
            long objSizesP50 = objSizes.get(objSizes.size() / 2);
            p90Index = (int)(objSizes.size() * 0.9);
            long objSizesP90 = objSizes.get(p90Index);

            objSizesPerfStats = new HashMap<String, String>() {{
                put("average", String.format("%.2f bytes", obAvgSize));
                put("p50", String.format("%d bytes", objSizesP50));
                put("p90", String.format("%d bytes", objSizesP90));
            }};
        }

        Map<String, Map<String, String>> perfStats = new HashMap<String, Map<String, String>>() {{
           put("latency", latencyPerfStats);
           put("throughput", tpPerfStats);
        }};

        if (objSizes != null) {
            perfStats.put("objectSize", objSizesPerfStats);
        }
        return perfStats;
    }

    /**
     * Generate Object names to be used in PUT, DELETE Object Perf.
     * @param numObjects number of objects
     * @return list of object names.
     */
    private List<String> generateKeyNames(int numObjects) {
        List<String> keys = new ArrayList<>();

        for (int i = 0; i < numObjects; i++) {
            String sb = "bolt-s3-perf" + i;
            keys.add(sb);
        }
        return keys;
    }

    /**
     * Generate a random string of certain length
     * @param objLength length of string
     * @return generated string
     */
    private String generate (int objLength) {
        String alphaNumString =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                "0123456789" +
                "abcdefghijklmnopqrstuvxyz";

        StringBuilder sb = new StringBuilder(objLength);
        SecureRandom random = new SecureRandom();

        for (int i = 0; i < objLength; i++) {
            int index = random.nextInt(alphaNumString.length());
            char nextChar = alphaNumString.charAt(index);
            sb.append(nextChar);
        }
        return sb.toString();
    }

    /**
     * Returns a list of `numKeys` objects from the given bucket in S3.
     * @param bucket bucket name
     * @return list of objects
     * @throws Exception
     */
    private List<String> listObjectsV2(String bucket) throws Exception {
        List<String> keys = new ArrayList<>();

        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucket)
                .maxKeys(numKeys)
                .build();

        ListObjectsV2Response resp;
        resp = s3.listObjectsV2(req);

        List<S3Object> objects = resp.contents();
        for (S3Object object : objects) {
            keys.add(object.key());
        }
        return keys;
    }
}
