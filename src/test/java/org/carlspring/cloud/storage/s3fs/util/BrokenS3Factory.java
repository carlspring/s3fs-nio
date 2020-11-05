package org.carlspring.cloud.storage.s3fs.util;

import org.carlspring.cloud.storage.s3fs.S3Factory;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class BrokenS3Factory
        extends S3Factory
{


    /**
     * @param name to make the constructor non default
     */
    public BrokenS3Factory(final String name)
    {
        // only non default constructor
    }

    @Override
    protected S3Client createS3Client(final S3ClientBuilder builder)
    {
        return null;
    }

}
