package org.carlspring.cloud.storage.s3fs.util;

import software.amazon.awssdk.awscore.client.builder.AwsDefaultClientBuilder;
import software.amazon.awssdk.awscore.client.config.AwsClientOption;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.interceptor.ClasspathInterceptorChainFactory;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4AuthScheme;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4aAuthScheme;
import software.amazon.awssdk.http.auth.scheme.NoAuthAuthScheme;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;
import software.amazon.awssdk.services.s3.auth.scheme.internal.S3AuthSchemeInterceptor;
import software.amazon.awssdk.services.s3.endpoints.S3ClientContextParams;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.awssdk.services.s3.endpoints.internal.S3RequestSetEndpointInterceptor;
import software.amazon.awssdk.services.s3.endpoints.internal.S3ResolveEndpointInterceptor;
import software.amazon.awssdk.services.s3.internal.S3ServiceClientConfigurationBuilder;
import software.amazon.awssdk.services.s3.internal.endpoints.UseGlobalEndpointResolver;
import software.amazon.awssdk.services.s3.internal.handlers.AsyncChecksumValidationInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.CreateBucketInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.CreateMultipartUploadRequestInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.DecodeUrlEncodedResponseInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.EnableTrailingChecksumInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.ExceptionTranslationInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.GetBucketPolicyInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.GetObjectInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.ObjectMetadataInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.S3ExpressChecksumInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.StreamingRequestInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.SyncChecksumValidationInterceptor;
import software.amazon.awssdk.services.s3.internal.plugins.S3DisableChunkEncodingIfConfiguredPlugin;
import software.amazon.awssdk.services.s3.internal.s3express.S3ExpressPlugin;
import software.amazon.awssdk.services.s3.internal.s3express.UseS3ExpressAuthResolver;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.awssdk.utils.Validate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class follows the {@link software.amazon.awssdk.services.s3.DefaultS3BaseClientBuilder} class which is not public so we can't extend it no expose it.
 *
 * @param <B>
 * @param <C>
 */
abstract class ExposingS3BaseClientBuilder<B extends S3BaseClientBuilder<B, C>, C>
        extends AwsDefaultClientBuilder<B, C>
{

    private final Map<String, AuthScheme<?>> additionalAuthSchemes = new HashMap();

    ExposingS3BaseClientBuilder()
    {
    }

    @Override
    protected final String serviceEndpointPrefix()
    {
        return "s3";
    }

    @Override
    protected final String serviceName()
    {
        return "S3";
    }

    @Override
    protected final SdkClientConfiguration mergeServiceDefaults(SdkClientConfiguration config) {
        return config.merge(c -> c.option(SdkClientOption.ENDPOINT_PROVIDER, defaultEndpointProvider())
                .option(SdkClientOption.AUTH_SCHEME_PROVIDER, defaultAuthSchemeProvider())
                .option(SdkClientOption.AUTH_SCHEMES, authSchemes())
                .option(SdkClientOption.CRC32_FROM_COMPRESSED_DATA_ENABLED, false)
                .option(SdkClientOption.SERVICE_CONFIGURATION, S3Configuration.builder().build()));
    }

    @Override
    protected final SdkClientConfiguration finalizeServiceConfiguration(SdkClientConfiguration config) {
        List<ExecutionInterceptor> endpointInterceptors = new ArrayList<>();
        endpointInterceptors.add(new S3AuthSchemeInterceptor());
        endpointInterceptors.add(new S3ResolveEndpointInterceptor());
        endpointInterceptors.add(new S3RequestSetEndpointInterceptor());
        endpointInterceptors.add(new StreamingRequestInterceptor());
        endpointInterceptors.add(new CreateBucketInterceptor());
        endpointInterceptors.add(new CreateMultipartUploadRequestInterceptor());
        endpointInterceptors.add(new DecodeUrlEncodedResponseInterceptor());
        endpointInterceptors.add(new GetBucketPolicyInterceptor());
        endpointInterceptors.add(new S3ExpressChecksumInterceptor());
        endpointInterceptors.add(new AsyncChecksumValidationInterceptor());
        endpointInterceptors.add(new SyncChecksumValidationInterceptor());
        endpointInterceptors.add(new EnableTrailingChecksumInterceptor());
        endpointInterceptors.add(new ExceptionTranslationInterceptor());
        endpointInterceptors.add(new GetObjectInterceptor());
        endpointInterceptors.add(new ObjectMetadataInterceptor());
        ClasspathInterceptorChainFactory interceptorFactory = new ClasspathInterceptorChainFactory();
        List<ExecutionInterceptor> interceptors = interceptorFactory
                .getInterceptors("software/amazon/awssdk/services/s3/execution.interceptors");
        List<ExecutionInterceptor> additionalInterceptors = new ArrayList<>();
        interceptors = CollectionUtils.mergeLists(endpointInterceptors, interceptors);
        interceptors = CollectionUtils.mergeLists(interceptors, additionalInterceptors);
        interceptors = CollectionUtils.mergeLists(interceptors, config.option(SdkClientOption.EXECUTION_INTERCEPTORS));
        S3Configuration.Builder serviceConfigBuilder = ((S3Configuration) config.option(SdkClientOption.SERVICE_CONFIGURATION))
                .toBuilder();
        serviceConfigBuilder.profileFile(serviceConfigBuilder.profileFileSupplier() != null ? serviceConfigBuilder
                .profileFileSupplier() : config.option(SdkClientOption.PROFILE_FILE_SUPPLIER));
        serviceConfigBuilder.profileName(serviceConfigBuilder.profileName() != null ? serviceConfigBuilder.profileName() : config
                .option(SdkClientOption.PROFILE_NAME));
        if (serviceConfigBuilder.dualstackEnabled() != null) {
            Validate.validState(
                    config.option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED) == null,
                    "Dualstack has been configured on both S3Configuration and the client/global level. Please limit dualstack configuration to one location.");
        } else {
            serviceConfigBuilder.dualstackEnabled(config.option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED));
        }
        if (serviceConfigBuilder.useArnRegionEnabled() != null) {
            Validate.validState(
                    clientContextParams.get(S3ClientContextParams.USE_ARN_REGION) == null,
                    "UseArnRegion has been configured on both S3Configuration and the client/global level. Please limit UseArnRegion configuration to one location.");
        } else {
            serviceConfigBuilder.useArnRegionEnabled(clientContextParams.get(S3ClientContextParams.USE_ARN_REGION));
        }
        if (serviceConfigBuilder.multiRegionEnabled() != null) {
            Validate.validState(
                    clientContextParams.get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS) == null,
                    "DisableMultiRegionAccessPoints has been configured on both S3Configuration and the client/global level. Please limit DisableMultiRegionAccessPoints configuration to one location.");
        } else if (clientContextParams.get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS) != null) {
            serviceConfigBuilder.multiRegionEnabled(!clientContextParams
                    .get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS));
        }
        if (serviceConfigBuilder.pathStyleAccessEnabled() != null) {
            Validate.validState(
                    clientContextParams.get(S3ClientContextParams.FORCE_PATH_STYLE) == null,
                    "ForcePathStyle has been configured on both S3Configuration and the client/global level. Please limit ForcePathStyle configuration to one location.");
        } else {
            serviceConfigBuilder.pathStyleAccessEnabled(clientContextParams.get(S3ClientContextParams.FORCE_PATH_STYLE));
        }
        if (serviceConfigBuilder.accelerateModeEnabled() != null) {
            Validate.validState(
                    clientContextParams.get(S3ClientContextParams.ACCELERATE) == null,
                    "Accelerate has been configured on both S3Configuration and the client/global level. Please limit Accelerate configuration to one location.");
        } else {
            serviceConfigBuilder.accelerateModeEnabled(clientContextParams.get(S3ClientContextParams.ACCELERATE));
        }
        S3Configuration finalServiceConfig = serviceConfigBuilder.build();
        clientContextParams.put(S3ClientContextParams.USE_ARN_REGION, finalServiceConfig.useArnRegionEnabled());
        clientContextParams.put(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS,
                !finalServiceConfig.multiRegionEnabled());
        clientContextParams.put(S3ClientContextParams.FORCE_PATH_STYLE, finalServiceConfig.pathStyleAccessEnabled());
        clientContextParams.put(S3ClientContextParams.ACCELERATE, finalServiceConfig.accelerateModeEnabled());
        UseGlobalEndpointResolver globalEndpointResolver = new UseGlobalEndpointResolver(config);
        UseS3ExpressAuthResolver useS3ExpressAuthResolver = new UseS3ExpressAuthResolver(config);
        if (clientContextParams.get(S3ClientContextParams.DISABLE_S3_EXPRESS_SESSION_AUTH) == null) {
            clientContextParams.put(S3ClientContextParams.DISABLE_S3_EXPRESS_SESSION_AUTH, !useS3ExpressAuthResolver.resolve());
        }
        SdkClientConfiguration.Builder builder = config.toBuilder();
        builder.lazyOption(SdkClientOption.IDENTITY_PROVIDERS, c -> {
            IdentityProviders.Builder result = IdentityProviders.builder();
            IdentityProvider<?> credentialsIdentityProvider = c.get(AwsClientOption.CREDENTIALS_IDENTITY_PROVIDER);
            if (credentialsIdentityProvider != null) {
                result.putIdentityProvider(credentialsIdentityProvider);
            }
            return result.build();
        });
        builder.option(SdkClientOption.EXECUTION_INTERCEPTORS, interceptors)
                .option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED, finalServiceConfig.dualstackEnabled())
                .option(SdkClientOption.SERVICE_CONFIGURATION, finalServiceConfig)
                .option(AwsClientOption.USE_GLOBAL_ENDPOINT,
                        globalEndpointResolver.resolve(config.option(AwsClientOption.AWS_REGION)))
                .option(SdkClientOption.CLIENT_CONTEXT_PARAMS, clientContextParams.build());
        return builder.build();
    }

    @Override
    protected final String signingName() {
        return "s3";
    }

    private S3EndpointProvider defaultEndpointProvider() {
        return S3EndpointProvider.defaultProvider();
    }

    public B authSchemeProvider(S3AuthSchemeProvider authSchemeProvider) {
        clientConfiguration.option(SdkClientOption.AUTH_SCHEME_PROVIDER, authSchemeProvider);
        return thisBuilder();
    }

    private S3AuthSchemeProvider defaultAuthSchemeProvider() {
        return S3AuthSchemeProvider.defaultProvider();
    }

    @Override
    public B putAuthScheme(AuthScheme<?> authScheme) {
        additionalAuthSchemes.put(authScheme.schemeId(), authScheme);
        return thisBuilder();
    }

    private Map<String, AuthScheme<?>> authSchemes() {
        Map<String, AuthScheme<?>> schemes = new HashMap<>(3 + this.additionalAuthSchemes.size());
        AwsV4AuthScheme awsV4AuthScheme = AwsV4AuthScheme.create();
        schemes.put(awsV4AuthScheme.schemeId(), awsV4AuthScheme);
        AwsV4aAuthScheme awsV4aAuthScheme = AwsV4aAuthScheme.create();
        schemes.put(awsV4aAuthScheme.schemeId(), awsV4aAuthScheme);
        NoAuthAuthScheme noAuthAuthScheme = NoAuthAuthScheme.create();
        schemes.put(noAuthAuthScheme.schemeId(), noAuthAuthScheme);
        schemes.putAll(this.additionalAuthSchemes);
        return schemes;
    }

    public B accelerate(Boolean accelerate) {
        clientContextParams.put(S3ClientContextParams.ACCELERATE, accelerate);
        return thisBuilder();
    }

    public B disableMultiRegionAccessPoints(Boolean disableMultiRegionAccessPoints) {
        clientContextParams.put(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS, disableMultiRegionAccessPoints);
        return thisBuilder();
    }

    public B disableS3ExpressSessionAuth(Boolean disableS3ExpressSessionAuth) {
        clientContextParams.put(S3ClientContextParams.DISABLE_S3_EXPRESS_SESSION_AUTH, disableS3ExpressSessionAuth);
        return thisBuilder();
    }

    public B forcePathStyle(Boolean forcePathStyle) {
        clientContextParams.put(S3ClientContextParams.FORCE_PATH_STYLE, forcePathStyle);
        return thisBuilder();
    }

    public B useArnRegion(Boolean useArnRegion) {
        clientContextParams.put(S3ClientContextParams.USE_ARN_REGION, useArnRegion);
        return thisBuilder();
    }

    public B crossRegionAccessEnabled(Boolean crossRegionAccessEnabled) {
        clientContextParams.put(S3ClientContextParams.CROSS_REGION_ACCESS_ENABLED, crossRegionAccessEnabled);
        return thisBuilder();
    }

    public B serviceConfiguration(S3Configuration serviceConfiguration) {
        clientConfiguration.option(SdkClientOption.SERVICE_CONFIGURATION, serviceConfiguration);
        return thisBuilder();
    }

    public void setServiceConfiguration(S3Configuration serviceConfiguration) {
        serviceConfiguration(serviceConfiguration);
    }

    @Override
    protected SdkClientConfiguration invokePlugins(SdkClientConfiguration config) {
        List<SdkPlugin> internalPlugins = internalPlugins(config);
        List<SdkPlugin> externalPlugins = plugins();
        if (internalPlugins.isEmpty() && externalPlugins.isEmpty()) {
            return config;
        }
        List<SdkPlugin> plugins = CollectionUtils.mergeLists(internalPlugins, externalPlugins);
        SdkClientConfiguration.Builder configuration = config.toBuilder();
        S3ServiceClientConfigurationBuilder serviceConfigBuilder = new S3ServiceClientConfigurationBuilder(configuration);
        for (SdkPlugin plugin : plugins) {
            plugin.configureClient(serviceConfigBuilder);
        }
        return configuration.build();
    }

    private List<SdkPlugin> internalPlugins(SdkClientConfiguration config) {
        List<SdkPlugin> internalPlugins = new ArrayList<>();
        internalPlugins.add(new S3DisableChunkEncodingIfConfiguredPlugin((config)));
        internalPlugins.add(new S3ExpressPlugin());
        return internalPlugins;
    }

    protected static void validateClientOptions(SdkClientConfiguration c) {
    }
}
