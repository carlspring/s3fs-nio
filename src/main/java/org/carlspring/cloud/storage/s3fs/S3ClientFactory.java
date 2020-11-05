package org.carlspring.cloud.storage.s3fs;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class S3ClientFactory
        extends S3Factory
{

    @Override
    protected S3Client createS3Client(final S3ClientBuilder builder)
    {
        return builder.build();
    }

}
