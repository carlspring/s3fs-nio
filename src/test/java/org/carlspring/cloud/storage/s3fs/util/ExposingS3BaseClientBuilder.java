package org.carlspring.cloud.storage.s3fs.util;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.awscore.client.builder.AwsDefaultClientBuilder;
import software.amazon.awssdk.awscore.client.config.AwsClientOption;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.interceptor.ClasspathInterceptorChainFactory;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3Configuration.Builder;
import software.amazon.awssdk.services.s3.endpoints.S3ClientContextParams;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.awssdk.services.s3.endpoints.internal.S3EndpointAuthSchemeInterceptor;
import software.amazon.awssdk.services.s3.endpoints.internal.S3RequestSetEndpointInterceptor;
import software.amazon.awssdk.services.s3.endpoints.internal.S3ResolveEndpointInterceptor;
import software.amazon.awssdk.services.s3.internal.endpoints.UseGlobalEndpointResolver;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.awssdk.utils.Validate;

/**
 * This class follows the {@link software.amazon.awssdk.services.s3.DefaultS3BaseClientBuilder} class which is not public so we can't extend it.
 *
 * @param <B>
 * @param <C>
 */
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

    protected final SdkClientConfiguration finalizeServiceConfiguration(SdkClientConfiguration config) {
        List<ExecutionInterceptor> endpointInterceptors = new ArrayList();
        endpointInterceptors.add(new S3ResolveEndpointInterceptor());
        endpointInterceptors.add(new S3EndpointAuthSchemeInterceptor());
        endpointInterceptors.add(new S3RequestSetEndpointInterceptor());
        ClasspathInterceptorChainFactory interceptorFactory = new ClasspathInterceptorChainFactory();
        List<ExecutionInterceptor> interceptors = interceptorFactory.getInterceptors("software/amazon/awssdk/services/s3/execution.interceptors");
        List<ExecutionInterceptor> additionalInterceptors = new ArrayList();
        interceptors = CollectionUtils.mergeLists(endpointInterceptors, interceptors);
        interceptors = CollectionUtils.mergeLists(interceptors, additionalInterceptors);
        interceptors = CollectionUtils.mergeLists(interceptors, (List)config.option(SdkClientOption.EXECUTION_INTERCEPTORS));
        S3Configuration.Builder serviceConfigBuilder = ((S3Configuration)config.option(SdkClientOption.SERVICE_CONFIGURATION)).toBuilder();
        serviceConfigBuilder.profileFile(serviceConfigBuilder.profileFile() != null ? serviceConfigBuilder.profileFile() : (ProfileFile)config.option(SdkClientOption.PROFILE_FILE));
        serviceConfigBuilder.profileName(serviceConfigBuilder.profileName() != null ? serviceConfigBuilder.profileName() : (String)config.option(SdkClientOption.PROFILE_NAME));
        if (serviceConfigBuilder.dualstackEnabled() != null) {
            Validate.validState(config.option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED) == null, "Dualstack has been configured on both S3Configuration and the client/global level. Please limit dualstack configuration to one location.", new Object[0]);
        } else {
            serviceConfigBuilder.dualstackEnabled((Boolean)config.option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED));
        }

        if (serviceConfigBuilder.useArnRegionEnabled() != null) {
            Validate.validState(this.clientContextParams.get(S3ClientContextParams.USE_ARN_REGION) == null, "UseArnRegion has been configured on both S3Configuration and the client/global level. Please limit UseArnRegion configuration to one location.", new Object[0]);
        } else {
            serviceConfigBuilder.useArnRegionEnabled((Boolean)this.clientContextParams.get(S3ClientContextParams.USE_ARN_REGION));
        }

        if (serviceConfigBuilder.multiRegionEnabled() != null) {
            Validate.validState(this.clientContextParams.get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS) == null, "DisableMultiRegionAccessPoints has been configured on both S3Configuration and the client/global level. Please limit DisableMultiRegionAccessPoints configuration to one location.", new Object[0]);
        } else if (this.clientContextParams.get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS) != null) {
            serviceConfigBuilder.multiRegionEnabled(!(Boolean)this.clientContextParams.get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS));
        }

        if (serviceConfigBuilder.pathStyleAccessEnabled() != null) {
            Validate.validState(this.clientContextParams.get(S3ClientContextParams.FORCE_PATH_STYLE) == null, "ForcePathStyle has been configured on both S3Configuration and the client/global level. Please limit ForcePathStyle configuration to one location.", new Object[0]);
        } else {
            serviceConfigBuilder.pathStyleAccessEnabled((Boolean)this.clientContextParams.get(S3ClientContextParams.FORCE_PATH_STYLE));
        }

        if (serviceConfigBuilder.accelerateModeEnabled() != null) {
            Validate.validState(this.clientContextParams.get(S3ClientContextParams.ACCELERATE) == null, "Accelerate has been configured on both S3Configuration and the client/global level. Please limit Accelerate configuration to one location.", new Object[0]);
        } else {
            serviceConfigBuilder.accelerateModeEnabled((Boolean)this.clientContextParams.get(S3ClientContextParams.ACCELERATE));
        }

        S3Configuration finalServiceConfig = (S3Configuration)serviceConfigBuilder.build();
        this.clientContextParams.put(S3ClientContextParams.USE_ARN_REGION, finalServiceConfig.useArnRegionEnabled());
        this.clientContextParams.put(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS, !finalServiceConfig.multiRegionEnabled());
        this.clientContextParams.put(S3ClientContextParams.FORCE_PATH_STYLE, finalServiceConfig.pathStyleAccessEnabled());
        this.clientContextParams.put(S3ClientContextParams.ACCELERATE, finalServiceConfig.accelerateModeEnabled());
        UseGlobalEndpointResolver resolver = new UseGlobalEndpointResolver(config);
        return config.toBuilder().option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED, finalServiceConfig.dualstackEnabled()).option(SdkClientOption.EXECUTION_INTERCEPTORS, interceptors).option(SdkClientOption.SERVICE_CONFIGURATION, finalServiceConfig).option(AwsClientOption.USE_GLOBAL_ENDPOINT, resolver.resolve((Region)config.option(AwsClientOption.AWS_REGION))).option(SdkClientOption.CLIENT_CONTEXT_PARAMS, this.clientContextParams.build()).build();
    }

    private Signer defaultSigner() {
        return AwsS3V4Signer.create();
    }

    protected final String signingName() {
        return "s3";
    }

    private S3EndpointProvider defaultEndpointProvider()
    {
        return S3EndpointProvider.defaultProvider();
    }

    public B accelerate(Boolean accelerate)
    {
        this.clientContextParams.put(S3ClientContextParams.ACCELERATE, accelerate);
        return this.thisBuilder();
    }

    public B disableMultiRegionAccessPoints(Boolean disableMultiRegionAccessPoints)
    {
        this.clientContextParams.put(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS, disableMultiRegionAccessPoints);
        return this.thisBuilder();
    }

    public B forcePathStyle(Boolean forcePathStyle)
    {
        this.clientContextParams.put(S3ClientContextParams.FORCE_PATH_STYLE, forcePathStyle);
        return this.thisBuilder();
    }

    public B useArnRegion(Boolean useArnRegion)
    {
        this.clientContextParams.put(S3ClientContextParams.USE_ARN_REGION, useArnRegion);
        return this.thisBuilder();
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
