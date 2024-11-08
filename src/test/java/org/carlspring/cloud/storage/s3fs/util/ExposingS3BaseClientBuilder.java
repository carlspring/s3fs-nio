package org.carlspring.cloud.storage.s3fs.util;

import software.amazon.awssdk.awscore.client.builder.AwsDefaultClientBuilder;
import software.amazon.awssdk.awscore.client.config.AwsClientOption;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.ServiceConfiguration;
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
import software.amazon.awssdk.regions.Region;
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
import software.amazon.awssdk.services.s3.internal.handlers.ConfigureSignerInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.CopySourceInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.CreateBucketInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.CreateMultipartUploadRequestInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.DecodeUrlEncodedResponseInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.DisablePayloadSigningInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.EnableChunkedEncodingInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.EnableTrailingChecksumInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.ExceptionTranslationInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.GetBucketPolicyInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.GetObjectInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.S3ExpressChecksumInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.StreamingRequestInterceptor;
import software.amazon.awssdk.services.s3.internal.handlers.SyncChecksumValidationInterceptor;
import software.amazon.awssdk.services.s3.internal.s3express.S3ExpressPlugin;
import software.amazon.awssdk.services.s3.internal.s3express.UseS3ExpressAuthResolver;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.awssdk.utils.Validate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This class follows the {@link software.amazon.awssdk.services.s3.DefaultS3BaseClientBuilder} class which is not public so we can't extend it.
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

        return config.merge((c) -> {
            c.option(SdkClientOption.ENDPOINT_PROVIDER, this.defaultEndpointProvider())
             .option(SdkClientOption.AUTH_SCHEME_PROVIDER, this.defaultAuthSchemeProvider())
             .option(SdkClientOption.AUTH_SCHEMES, this.authSchemes())
             .option(SdkClientOption.CRC32_FROM_COMPRESSED_DATA_ENABLED, false)
             .option(SdkClientOption.SERVICE_CONFIGURATION, (ServiceConfiguration) S3Configuration.builder().build());
        });

    }

    protected final SdkClientConfiguration finalizeServiceConfiguration(SdkClientConfiguration config) {
        List<ExecutionInterceptor> endpointInterceptors = new ArrayList();
        endpointInterceptors.add(new S3AuthSchemeInterceptor());
        endpointInterceptors.add(new S3ResolveEndpointInterceptor());
        endpointInterceptors.add(new S3RequestSetEndpointInterceptor());
        endpointInterceptors.add(new StreamingRequestInterceptor());
        endpointInterceptors.add(new CreateBucketInterceptor());
        endpointInterceptors.add(new CreateMultipartUploadRequestInterceptor());
        endpointInterceptors.add(new EnableChunkedEncodingInterceptor());
        endpointInterceptors.add(new ConfigureSignerInterceptor());
        endpointInterceptors.add(new DecodeUrlEncodedResponseInterceptor());
        endpointInterceptors.add(new GetBucketPolicyInterceptor());
        endpointInterceptors.add(new S3ExpressChecksumInterceptor());
        endpointInterceptors.add(new AsyncChecksumValidationInterceptor());
        endpointInterceptors.add(new SyncChecksumValidationInterceptor());
        endpointInterceptors.add(new EnableTrailingChecksumInterceptor());
        endpointInterceptors.add(new ExceptionTranslationInterceptor());
        endpointInterceptors.add(new GetObjectInterceptor());
        endpointInterceptors.add(new CopySourceInterceptor());
        endpointInterceptors.add(new DisablePayloadSigningInterceptor());
        ClasspathInterceptorChainFactory interceptorFactory = new ClasspathInterceptorChainFactory();
        List<ExecutionInterceptor> interceptors = interceptorFactory.getInterceptors("software/amazon/awssdk/services/s3/execution.interceptors");
        List<ExecutionInterceptor> additionalInterceptors = new ArrayList<>();
        interceptors = CollectionUtils.mergeLists(endpointInterceptors, interceptors);
        interceptors = CollectionUtils.mergeLists(interceptors, additionalInterceptors);
        interceptors = CollectionUtils.mergeLists(interceptors, config.option(SdkClientOption.EXECUTION_INTERCEPTORS));
        S3Configuration.Builder serviceConfigBuilder = ((S3Configuration)config.option(SdkClientOption.SERVICE_CONFIGURATION)).toBuilder();
        serviceConfigBuilder.profileFile(serviceConfigBuilder.profileFileSupplier() != null ? serviceConfigBuilder.profileFileSupplier() : (Supplier)config.option(SdkClientOption.PROFILE_FILE_SUPPLIER));
        serviceConfigBuilder.profileName(serviceConfigBuilder.profileName() != null ? serviceConfigBuilder.profileName() : config.option(SdkClientOption.PROFILE_NAME));
        if (serviceConfigBuilder.dualstackEnabled() != null) {
            Validate.validState(config.option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED) == null, "Dualstack has been configured on both S3Configuration and the client/global level. Please limit dualstack configuration to one location.", new Object[0]);
        } else {
            serviceConfigBuilder.dualstackEnabled(config.option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED));
        }

        if (serviceConfigBuilder.useArnRegionEnabled() != null) {
            Validate.validState(this.clientContextParams.get(S3ClientContextParams.USE_ARN_REGION) == null, "UseArnRegion has been configured on both S3Configuration and the client/global level. Please limit UseArnRegion configuration to one location.", new Object[0]);
        } else {
            serviceConfigBuilder.useArnRegionEnabled(this.clientContextParams.get(S3ClientContextParams.USE_ARN_REGION));
        }

        if (serviceConfigBuilder.multiRegionEnabled() != null) {
            Validate.validState(this.clientContextParams.get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS) == null, "DisableMultiRegionAccessPoints has been configured on both S3Configuration and the client/global level. Please limit DisableMultiRegionAccessPoints configuration to one location.", new Object[0]);
        } else if (this.clientContextParams.get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS) != null) {
            serviceConfigBuilder.multiRegionEnabled(!(Boolean)this.clientContextParams.get(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS));
        }

        if (serviceConfigBuilder.pathStyleAccessEnabled() != null) {
            Validate.validState(this.clientContextParams.get(S3ClientContextParams.FORCE_PATH_STYLE) == null, "ForcePathStyle has been configured on both S3Configuration and the client/global level. Please limit ForcePathStyle configuration to one location.", new Object[0]);
        } else {
            serviceConfigBuilder.pathStyleAccessEnabled(this.clientContextParams.get(S3ClientContextParams.FORCE_PATH_STYLE));
        }

        if (serviceConfigBuilder.accelerateModeEnabled() != null) {
            Validate.validState(this.clientContextParams.get(S3ClientContextParams.ACCELERATE) == null, "Accelerate has been configured on both S3Configuration and the client/global level. Please limit Accelerate configuration to one location.", new Object[0]);
        } else {
            serviceConfigBuilder.accelerateModeEnabled(this.clientContextParams.get(S3ClientContextParams.ACCELERATE));
        }

        S3Configuration finalServiceConfig = serviceConfigBuilder.build();
        this.clientContextParams.put(S3ClientContextParams.USE_ARN_REGION, finalServiceConfig.useArnRegionEnabled());
        this.clientContextParams.put(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS, !finalServiceConfig.multiRegionEnabled());
        this.clientContextParams.put(S3ClientContextParams.FORCE_PATH_STYLE, finalServiceConfig.pathStyleAccessEnabled());
        this.clientContextParams.put(S3ClientContextParams.ACCELERATE, finalServiceConfig.accelerateModeEnabled());
        UseGlobalEndpointResolver globalEndpointResolver = new UseGlobalEndpointResolver(config);
        UseS3ExpressAuthResolver useS3ExpressAuthResolver = new UseS3ExpressAuthResolver(config);
        if (this.clientContextParams.get(S3ClientContextParams.DISABLE_S3_EXPRESS_SESSION_AUTH) == null) {
            this.clientContextParams.put(S3ClientContextParams.DISABLE_S3_EXPRESS_SESSION_AUTH, !useS3ExpressAuthResolver.resolve());
        }

        SdkClientConfiguration.Builder builder = config.toBuilder();
        builder.lazyOption(SdkClientOption.IDENTITY_PROVIDERS, (c) -> {
            IdentityProviders.Builder result = IdentityProviders.builder();
            IdentityProvider<?> credentialsIdentityProvider = c.get(AwsClientOption.CREDENTIALS_IDENTITY_PROVIDER);
            if (credentialsIdentityProvider != null) {
                result.putIdentityProvider(credentialsIdentityProvider);
            }

            return (IdentityProviders)result.build();
        });
        builder.option(SdkClientOption.EXECUTION_INTERCEPTORS, interceptors).option(AwsClientOption.DUALSTACK_ENDPOINT_ENABLED, finalServiceConfig.dualstackEnabled()).option(SdkClientOption.SERVICE_CONFIGURATION, finalServiceConfig).option(AwsClientOption.USE_GLOBAL_ENDPOINT, globalEndpointResolver.resolve((Region)config.option(AwsClientOption.AWS_REGION))).option(SdkClientOption.CLIENT_CONTEXT_PARAMS, this.clientContextParams.build());
        return builder.build();
    }

    protected final String signingName() {
        return "s3";
    }

    private S3EndpointProvider defaultEndpointProvider() {
        return S3EndpointProvider.defaultProvider();
    }

    public B authSchemeProvider(S3AuthSchemeProvider authSchemeProvider) {
        this.clientConfiguration.option(SdkClientOption.AUTH_SCHEME_PROVIDER, authSchemeProvider);
        return this.thisBuilder();
    }

    private S3AuthSchemeProvider defaultAuthSchemeProvider() {
        return S3AuthSchemeProvider.defaultProvider();
    }

    public B putAuthScheme(AuthScheme<?> authScheme) {
        this.additionalAuthSchemes.put(authScheme.schemeId(), authScheme);
        return this.thisBuilder();
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
        this.clientContextParams.put(S3ClientContextParams.ACCELERATE, accelerate);
        return this.thisBuilder();
    }

    public B disableMultiRegionAccessPoints(Boolean disableMultiRegionAccessPoints)
    {
        this.clientContextParams.put(S3ClientContextParams.DISABLE_MULTI_REGION_ACCESS_POINTS, disableMultiRegionAccessPoints);
        return this.thisBuilder();
    }

    public B disableS3ExpressSessionAuth(Boolean disableS3ExpressSessionAuth) {
        this.clientContextParams.put(S3ClientContextParams.DISABLE_S3_EXPRESS_SESSION_AUTH, disableS3ExpressSessionAuth);
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

    public B crossRegionAccessEnabled(Boolean crossRegionAccessEnabled) {
        this.clientContextParams.put(S3ClientContextParams.CROSS_REGION_ACCESS_ENABLED, crossRegionAccessEnabled);
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

    protected SdkClientConfiguration invokePlugins(SdkClientConfiguration config) {
        List<SdkPlugin> internalPlugins = this.internalPlugins();
        List<SdkPlugin> externalPlugins = this.plugins();
        if (internalPlugins.isEmpty() && externalPlugins.isEmpty()) {
            return config;
        } else {
            List<SdkPlugin> plugins = CollectionUtils.mergeLists(internalPlugins, externalPlugins);
            SdkClientConfiguration.Builder configuration = config.toBuilder();
            S3ServiceClientConfigurationBuilder serviceConfigBuilder = new S3ServiceClientConfigurationBuilder(configuration);
            Iterator var7 = plugins.iterator();

            while(var7.hasNext()) {
                SdkPlugin plugin = (SdkPlugin)var7.next();
                plugin.configureClient(serviceConfigBuilder);
            }

            return configuration.build();
        }
    }

    private List<SdkPlugin> internalPlugins() {
        List<SdkPlugin> internalPlugins = new ArrayList();
        internalPlugins.add(new S3ExpressPlugin());
        return internalPlugins;
    }

    protected static void validateClientOptions(SdkClientConfiguration c) {
    }
}
