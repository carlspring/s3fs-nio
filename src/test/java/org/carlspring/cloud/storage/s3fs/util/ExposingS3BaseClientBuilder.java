package org.carlspring.cloud.storage.s3fs.util;

import java.util.List;

import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.awscore.client.builder.AwsDefaultClientBuilder;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.interceptor.ClasspathInterceptorChainFactory;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3Configuration.Builder;
import software.amazon.awssdk.utils.CollectionUtils;

abstract class ExposingS3BaseClientBuilder<B extends S3BaseClientBuilder<B, C>, C>
        extends AwsDefaultClientBuilder<B, C>
{

    ExposingS3BaseClientBuilder()
    {
    }

    protected final String serviceEndpointPrefix()
    {
        return "s3";
    }

    protected final String serviceName()
    {
        return "S3";
    }

    protected final SdkClientConfiguration mergeServiceDefaults(SdkClientConfiguration config)
    {
        return config.merge((c) -> c.option(SdkAdvancedClientOption.SIGNER, this.defaultSigner()).option(
                SdkClientOption.CRC32_FROM_COMPRESSED_DATA_ENABLED, false).option(
                SdkClientOption.SERVICE_CONFIGURATION, S3Configuration.builder().build()));
    }

    protected final SdkClientConfiguration finalizeServiceConfiguration(SdkClientConfiguration config)
    {
        ClasspathInterceptorChainFactory interceptorFactory = new ClasspathInterceptorChainFactory();
        List<ExecutionInterceptor> interceptors = interceptorFactory.getInterceptors(
                "software/amazon/awssdk/services/s3/execution.interceptors");
        interceptors = CollectionUtils.mergeLists(interceptors,
                                                  config.option(SdkClientOption.EXECUTION_INTERCEPTORS));
        Builder c = ((S3Configuration) config.option(SdkClientOption.SERVICE_CONFIGURATION)).toBuilder();
        c.profileFile(c.profileFile() != null ? c.profileFile() :
                      config.option(SdkClientOption.PROFILE_FILE)).profileName(
                c.profileName() != null ? c.profileName() : config.option(SdkClientOption.PROFILE_NAME));
        return config.toBuilder().option(SdkClientOption.EXECUTION_INTERCEPTORS, interceptors).option(
                SdkClientOption.SERVICE_CONFIGURATION, c.build()).build();
    }

    private Signer defaultSigner()
    {
        return AwsS3V4Signer.create();
    }

    protected final String signingName()
    {
        return "s3";
    }

    public B serviceConfiguration(S3Configuration serviceConfiguration)
    {
        this.clientConfiguration.option(SdkClientOption.SERVICE_CONFIGURATION, serviceConfiguration);
        return this.thisBuilder();
    }

    public void setServiceConfiguration(S3Configuration serviceConfiguration)
    {
        this.serviceConfiguration(serviceConfiguration);
    }
}
