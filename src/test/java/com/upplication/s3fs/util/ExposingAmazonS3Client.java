package com.upplication.s3fs.util;

import java.net.URI;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;

public class ExposingAmazonS3Client extends AmazonS3Client {

    private AWSCredentialsProvider awsCredentialsProvider;
    private S3ClientOptions s3ClientOptions = S3ClientOptions.builder().build();

    public ExposingAmazonS3Client(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, RequestMetricCollector requestMetricsCollector) {
        super(credentialsProvider, clientConfiguration, requestMetricsCollector);
        this.awsCredentialsProvider = credentialsProvider;
    }

    public AWSCredentialsProvider getAWSCredentialsProvider() {
        return awsCredentialsProvider;
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }


    public synchronized void setS3ClientOptions(S3ClientOptions clientOptions) {
        super.setS3ClientOptions(clientOptions);
        this.s3ClientOptions = clientOptions;
    }

    public S3ClientOptions getClientOptions() {
        return this.s3ClientOptions;
    }

    public RequestMetricCollector getRequestMetricCollector() {
        return super.requestMetricCollector();
    }

    public URI getEndpoint() {
        return endpoint;
    }
}