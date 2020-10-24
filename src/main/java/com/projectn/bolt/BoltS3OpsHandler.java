package com.projectn.bolt;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.util.Map;

public class BoltS3OpsHandler implements RequestHandler<Map<String,String>, Map<String,String>> {

    @Override
    public Map<String,String> handleRequest(Map<String,String> event, Context context) {

        BoltS3OpsClient boltS3OpsClient = new BoltS3OpsClient();
        return boltS3OpsClient.processEvent(event);
    }
}
