package com.upplication.s3fs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.AmazonS3Client;

public class AmazonS3ClientFactory extends AmazonS3Factory {
	@Override
	protected AmazonS3Client createAmazonS3(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration, RequestMetricCollector requestMetricsCollector) {
		return new AmazonS3Client(credentialsProvider, clientConfiguration, requestMetricsCollector);
	}
}