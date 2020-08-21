package org.carlspring.cloud.storage.s3fs.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3;
import org.carlspring.cloud.storage.s3fs.AmazonS3Factory;

public class BrokenAmazonS3Factory extends AmazonS3Factory {
    /**
     * @param name to make the constructor non default
     */
    public BrokenAmazonS3Factory(String name) {
        // only non default constructor
    }

    @Override
    protected AmazonS3 createAmazonS3(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, RequestMetricCollector requestMetricsCollector) {
        return null;
    }
}
