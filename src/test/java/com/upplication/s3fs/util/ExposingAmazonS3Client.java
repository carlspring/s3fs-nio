package com.upplication.s3fs.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3Client;

public class ExposingAmazonS3Client extends AmazonS3Client {
	private AWSCredentialsProvider awsCredentialsProvider;

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

	public RequestMetricCollector getRequestMetricCollector() {
		return super.requestMetricCollector();
	}
}