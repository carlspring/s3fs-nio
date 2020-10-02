package org.carlspring.cloud.storage.s3fs.util;

import software.amazon.awssdk.awscore.client.handler.AwsSyncClientHandler;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.handler.SyncClientHandler;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class ExposingS3Client
        implements S3Client
{

    private final SyncClientHandler clientHandler;
    private final SdkClientConfiguration clientConfiguration;

    public ExposingS3Client(SdkClientConfiguration clientConfiguration)
    {
        this.clientHandler = new AwsSyncClientHandler(clientConfiguration);
        this.clientConfiguration = clientConfiguration;
    }

    public SdkClientConfiguration getClientConfiguration()
    {
        return clientConfiguration;
    }

    static S3ClientBuilder builder() {
        return new ExposingS3ClientBuilder();
    }

    @Override
    public String serviceName()
    {
        return SERVICE_NAME;
    }

    @Override
    public void close()
    {
        this.clientHandler.close();
    }
}
