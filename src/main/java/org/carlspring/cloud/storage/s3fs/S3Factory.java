package org.carlspring.cloud.storage.s3fs;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Protocol;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.SIGNER;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;


/**
 * Factory base class to create a new S3Client instance.
 */
public abstract class S3Factory
{

    public static final String REGION = "s3fs_region";

    public static final String ACCESS_KEY = "s3fs_access_key";

    public static final String SECRET_KEY = "s3fs_secret_key";

    public static final String REQUEST_METRIC_COLLECTOR_CLASS = "s3fs_request_metric_collector_class";

    public static final String CONNECTION_TIMEOUT = "s3fs_connection_timeout";

    public static final String MAX_CONNECTIONS = "s3fs_max_connections";

    public static final String MAX_ERROR_RETRY = "s3fs_max_retry_error";

    public static final String PROTOCOL = "s3fs_protocol";

    public static final String PROXY_DOMAIN = "s3fs_proxy_domain";

    public static final String PROXY_HOST = "s3fs_proxy_host";

    public static final String PROXY_PASSWORD = "s3fs_proxy_password";

    public static final String PROXY_PORT = "s3fs_proxy_port";

    public static final String PROXY_USERNAME = "s3fs_proxy_username";

    public static final String PROXY_WORKSTATION = "s3fs_proxy_workstation";

    /**
     * @deprecated Not supported according to https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#133-client-override-configuration
     */
    @Deprecated
    public static final String SOCKET_SEND_BUFFER_SIZE_HINT = "s3fs_socket_send_buffer_size_hint";

    /**
     * @deprecated Not supported according to https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#133-client-override-configuration
     */
    @Deprecated
    public static final String SOCKET_RECEIVE_BUFFER_SIZE_HINT = "s3fs_socket_receive_buffer_size_hint";

    public static final String SOCKET_TIMEOUT = "s3fs_socket_timeout";

    public static final String USER_AGENT = "s3fs_user_agent";

    public static final String SIGNER_OVERRIDE = "s3fs_signer_override";

    public static final String PATH_STYLE_ACCESS = "s3fs_path_style_access";

    private static final Logger LOGGER = LoggerFactory.getLogger(S3Factory.class);

    private static final String DEFAULT_PROTOCOL = Protocol.HTTPS.toString();

    private static final Region DEFAULT_REGION = Region.US_EAST_1;

    private static final int RADIX = 10;

    /**
     * Build a new Amazon S3 instance with the URI and the properties provided
     *
     * @param uri   URI mandatory
     * @param props Properties with the credentials and others options
     * @return {@link software.amazon.awssdk.services.s3.S3Client}
     */
    public S3Client getS3Client(final URI uri,
                                final Properties props)
    {
        S3ClientBuilder builder = getS3ClientBuilder();

        if (uri.getHost() != null)
        {
            final URI endpoint = getEndpointUri(uri.getHost(), uri.getPort(), props);
            builder.endpointOverride(endpoint);
        }

        builder.region(getRegion(props))
               .credentialsProvider(getCredentialsProvider(props))
               .httpClient(getHttpClient(props))
               .overrideConfiguration(getOverrideConfiguration(props))
               .serviceConfiguration(getServiceConfiguration(props));

        return createS3Client(builder);
    }

    private URI getEndpointUri(final String host,
                               final int port,
                               final Properties props)
    {
        final String scheme = getProtocol(props);

        String endpointStr;
        if (port != -1)
        {
            endpointStr = String.format("%s://%s:%d", scheme, host, port);
        }
        else
        {
            endpointStr = String.format("%s://%s", scheme, host);
        }

        return URI.create(endpointStr);
    }

    protected S3ClientBuilder getS3ClientBuilder()
    {
        return S3Client.builder();
    }

    private String getProtocol(final Properties props)
    {
        if (props.getProperty(PROTOCOL) != null)
        {
            return Protocol.fromValue(props.getProperty(PROTOCOL)).toString();
        }

        return DEFAULT_PROTOCOL;
    }

    private Region getRegion(final Properties props)
    {
        if (props.getProperty(REGION) != null)
        {
            return Region.of(props.getProperty(REGION));
        }

        try
        {
            return new DefaultAwsRegionProviderChain().getRegion();
        }
        catch (final SdkClientException e)
        {
            LOGGER.warn("Unable to load region from any of the providers in the chain");
        }

        return DEFAULT_REGION;
    }

    protected SdkHttpClient getHttpClient(final Properties props)
    {
        final ApacheHttpClient.Builder builder = getApacheHttpClientBuilder(props);
        return builder.build();
    }

    private ApacheHttpClient.Builder getApacheHttpClientBuilder(final Properties props)
    {
        final ApacheHttpClient.Builder builder = ApacheHttpClient.builder();

        if (props.getProperty(CONNECTION_TIMEOUT) != null)
        {
            try
            {
                final Duration duration = Duration.ofMillis(
                        Long.parseLong(props.getProperty(CONNECTION_TIMEOUT), RADIX));
                builder.connectionTimeout(duration);
            }
            catch (final NumberFormatException e)
            {
                printWarningMessage(props, CONNECTION_TIMEOUT);
            }
        }

        if (props.getProperty(MAX_CONNECTIONS) != null)
        {
            try
            {
                final int maxConnections = Integer.parseInt(props.getProperty(MAX_CONNECTIONS), RADIX);
                builder.maxConnections(maxConnections);
            }
            catch (final NumberFormatException e)
            {
                printWarningMessage(props, MAX_CONNECTIONS);
            }
        }

        if (props.getProperty(SOCKET_TIMEOUT) != null)
        {
            try
            {
                final Duration duration = Duration.ofMillis(Long.parseLong(props.getProperty(SOCKET_TIMEOUT), RADIX));
                builder.socketTimeout(duration);
            }
            catch (final NumberFormatException e)
            {
                printWarningMessage(props, SOCKET_TIMEOUT);
            }
        }

        return builder.proxyConfiguration(getProxyConfiguration(props));
    }

    /**
     * should return a new S3Client
     *
     * @param builder S3ClientBuilder mandatory.
     * @return {@link software.amazon.awssdk.services.s3.S3Client}
     */
    protected abstract S3Client createS3Client(final S3ClientBuilder builder);

    protected AwsCredentialsProvider getCredentialsProvider(final Properties props)
    {
        AwsCredentialsProvider credentialsProvider;
        if (props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null)
        {
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        else
        {
            final AwsCredentials awsCredentials = getAwsCredentials(props);
            credentialsProvider = StaticCredentialsProvider.create(awsCredentials);
        }
        return credentialsProvider;
    }

    protected AwsCredentials getAwsCredentials(final Properties props)
    {
        return AwsBasicCredentials.create(props.getProperty(ACCESS_KEY), props.getProperty(SECRET_KEY));
    }

    protected S3Configuration getServiceConfiguration(final Properties props)
    {
        S3Configuration.Builder builder = S3Configuration.builder();
        if (props.getProperty(PATH_STYLE_ACCESS) != null && Boolean.parseBoolean(props.getProperty(PATH_STYLE_ACCESS)))
        {
            builder.pathStyleAccessEnabled(true);
        }

        return builder.build();
    }

    protected ClientOverrideConfiguration getOverrideConfiguration(final Properties props)
    {
        final ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();

        if (props.getProperty(MAX_ERROR_RETRY) != null)
        {
            try
            {
                final Integer numRetries = Integer.parseInt(props.getProperty(MAX_ERROR_RETRY), RADIX);
                final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(numRetries).build();
                builder.retryPolicy(retryPolicy);
            }
            catch (final NumberFormatException e)
            {
                printWarningMessage(props, MAX_ERROR_RETRY);
            }
        }

        if (props.getProperty(USER_AGENT) != null)
        {
            builder.putAdvancedOption(USER_AGENT_PREFIX, props.getProperty(USER_AGENT));
        }

        if (props.getProperty(SIGNER_OVERRIDE) != null)
        {
            try
            {
                final Class<?> clazz = Class.forName(props.getProperty(SIGNER_OVERRIDE));
                final Signer signer = (Signer) clazz.getDeclaredConstructor().newInstance();
                builder.putAdvancedOption(SIGNER, signer);
            }
            catch (final ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e)
            {
                printWarningMessage(props, SIGNER_OVERRIDE);
            }
        }

        return builder.build();
    }


    protected ProxyConfiguration getProxyConfiguration(final Properties props)
    {
        final ProxyConfiguration.Builder builder = ProxyConfiguration.builder();

        if (props.getProperty(PROXY_HOST) != null)
        {
            final String host = props.getProperty(PROXY_HOST);
            final String portStr = props.getProperty(PROXY_PORT);
            int port = -1;
            try
            {
                port = portStr != null ? Integer.parseInt(portStr, RADIX) : -1;
            }
            catch (final NumberFormatException e)
            {
                printWarningMessage(props, PROXY_PORT);
            }

            final URI uri = getEndpointUri(host, port, props);
            builder.endpoint(uri);
        }

        if (props.getProperty(PROXY_USERNAME) != null)
        {
            builder.username(props.getProperty(PROXY_USERNAME));
        }

        if (props.getProperty(PROXY_PASSWORD) != null)
        {
            builder.password(props.getProperty(PROXY_PASSWORD));
        }

        if (props.getProperty(PROXY_DOMAIN) != null)
        {
            builder.ntlmDomain(props.getProperty(PROXY_DOMAIN));
        }

        if (props.getProperty(PROXY_WORKSTATION) != null)
        {
            builder.ntlmWorkstation(props.getProperty(PROXY_WORKSTATION));
        }

        return builder.build();
    }

    private void printWarningMessage(final Properties props,
                                     final String propertyName)
    {
        LOGGER.warn("The '{}' property could not be loaded with this value: {}", propertyName,
                    props.getProperty(propertyName));
    }

}
