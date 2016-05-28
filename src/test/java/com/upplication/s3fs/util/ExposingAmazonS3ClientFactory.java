package com.upplication.s3fs.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3Client;
import com.upplication.s3fs.AmazonS3ClientFactory;

public class ExposingAmazonS3ClientFactory extends AmazonS3ClientFactory {
    @Override
    protected AmazonS3Client createAmazonS3(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, RequestMetricCollector requestMetricsCollector) {
        return new ExposingAmazonS3Client(credentialsProvider, clientConfiguration, requestMetricsCollector);
    }
}