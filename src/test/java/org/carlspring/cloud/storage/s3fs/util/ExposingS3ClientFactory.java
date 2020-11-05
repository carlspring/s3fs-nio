package org.carlspring.cloud.storage.s3fs.util;

import org.carlspring.cloud.storage.s3fs.S3ClientFactory;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class ExposingS3ClientFactory
        extends S3ClientFactory
{

    @Override
    protected S3Client createS3Client(final S3ClientBuilder builder)
    {
        return builder.build();
    }

    @Override
    protected S3ClientBuilder getS3ClientBuilder()
    {
        return ExposingS3Client.builder();
    }

}
