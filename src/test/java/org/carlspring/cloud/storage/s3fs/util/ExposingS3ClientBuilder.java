package org.carlspring.cloud.storage.s3fs.util;

import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;

/**
 * This class follows the {@link software.amazon.awssdk.services.s3.DefaultS3ClientBuilder} implementation which is not public.
 */
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


    public ExposingS3ClientBuilder endpointProvider(S3EndpointProvider endpointProvider) {
        this.clientConfiguration.option(SdkClientOption.ENDPOINT_PROVIDER, endpointProvider);
        return this;
    }

}
