package org.carlspring.cloud.storage.s3fs.util;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class ExposingS3ClientBuilder
        extends ExposingS3BaseClientBuilder<S3ClientBuilder, S3Client>
        implements S3ClientBuilder
{

    ExposingS3ClientBuilder()
    {
    }

    protected final S3Client buildClient()
    {
        return new ExposingS3Client(super.syncClientConfiguration());
    }
}
