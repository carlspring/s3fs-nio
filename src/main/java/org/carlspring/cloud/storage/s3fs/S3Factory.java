package org.carlspring.cloud.storage.s3fs;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.utils.AttributeMap;


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
        String endpointStr;
        if (port != -1)
        {
            endpointStr = String.format("%s:%d", host, port);
        }
        else
        {
            endpointStr = host;
        }

        // Endpoint may not have an scheme. E.g: 127.0.0.1:12345
        if (!endpointStr.contains("://"))
        {
            final String protocol = getProtocol(props);
            endpointStr = String.format("%s://%s", protocol, endpointStr);
        }

        return URI.create(endpointStr);
    }

    protected S3ClientBuilder getS3ClientBuilder()
    {
        return S3Client.builder();
    }

    private String getProtocol(final Properties props)
    {
        return props.getProperty(PROTOCOL);
    }

    private Region getRegion(final Properties props)
    {
        if (props.getProperty(REGION) != null)
        {
            return Region.of(props.getProperty(REGION));
        }

        return null;
    }

    protected SdkHttpClient getHttpClient(final Properties props)
    {
        final ApacheHttpClient.Builder builder = getApacheHttpClientBuilder(props);
        final AttributeMap map = getAttributeMap(props);
        return builder.buildWithDefaults(map);
    }

    private ApacheHttpClient.Builder getApacheHttpClientBuilder(final Properties props)
    {
        final ApacheHttpClient.Builder builder = ApacheHttpClient.builder();

        if (props.getProperty(CONNECTION_TIMEOUT) != null)
        {
            final Duration duration = Duration.ofMillis(Long.parseLong(props.getProperty(CONNECTION_TIMEOUT)));
            builder.connectionTimeout(duration);
        }

        if (props.getProperty(MAX_CONNECTIONS) != null)
        {
            builder.maxConnections(Integer.parseInt(props.getProperty(MAX_CONNECTIONS)));
        }

        return builder.proxyConfiguration(getProxyConfiguration(props));
    }

    private AttributeMap getAttributeMap(Properties props)
    {
        final AttributeMap.Builder builder = AttributeMap.builder();
        if (props.getProperty(SOCKET_TIMEOUT) != null)
        {
            final Duration duration = Duration.ofMillis(Long.parseLong(props.getProperty(SOCKET_TIMEOUT)));
            builder.put(SdkHttpConfigurationOption.READ_TIMEOUT, duration);
        }
        return builder.build();
    }

    /**
     * should return a new S3Client
     *
     * @param builder S3ClientBuilder mandatory.
     * @return {@link software.amazon.awssdk.services.s3.S3Client}
     */
    protected abstract S3Client createS3Client(final S3ClientBuilder builder);

    protected AwsCredentialsProvider getCredentialsProvider(Properties props)
    {
        AwsCredentialsProvider credentialsProvider;
        if (props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null)
        {
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        else
        {
            credentialsProvider = StaticCredentialsProvider.create(getAwsCredentials(props));
        }
        return credentialsProvider;
    }

    protected AwsCredentials getAwsCredentials(Properties props)
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

    protected ClientOverrideConfiguration getOverrideConfiguration(Properties props)
    {
        final ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();

        if (props.getProperty(MAX_ERROR_RETRY) != null)
        {
            final Integer numRetries = Integer.parseInt(props.getProperty(MAX_ERROR_RETRY));
            final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(numRetries).build();
            builder.retryPolicy(retryPolicy);
        }

        return builder.build();
    }


    protected ProxyConfiguration getProxyConfiguration(final Properties props)
    {
        final ProxyConfiguration.Builder builder = ProxyConfiguration.builder();

        if (props.getProperty(PROXY_HOST) != null)
        {
            final String host = props.getProperty(PROXY_HOST);
            final int port =
                    props.getProperty(PROXY_PORT) != null ? Integer.parseInt(props.getProperty(PROXY_PORT)) : -1;
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

}
