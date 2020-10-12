package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.util.ExposingS3Client;
import org.carlspring.cloud.storage.s3fs.util.ExposingS3ClientFactory;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.net.URI;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.s3.S3Configuration;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.CONNECTION_TIMEOUT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.MAX_CONNECTIONS;
import static org.carlspring.cloud.storage.s3fs.S3Factory.MAX_ERROR_RETRY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PATH_STYLE_ACCESS;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROTOCOL;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_DOMAIN;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_HOST;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_PASSWORD;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_PORT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_USERNAME;
import static org.carlspring.cloud.storage.s3fs.S3Factory.PROXY_WORKSTATION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REGION;
import static org.carlspring.cloud.storage.s3fs.S3Factory.REQUEST_METRIC_COLLECTOR_CLASS;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SIGNER_OVERRIDE;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SOCKET_RECEIVE_BUFFER_SIZE_HINT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SOCKET_SEND_BUFFER_SIZE_HINT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SOCKET_TIMEOUT;
import static org.carlspring.cloud.storage.s3fs.S3Factory.USER_AGENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.awssdk.core.client.config.SdkClientOption.ENDPOINT;

class S3ClientFactoryTest
{
    @Test
    void neverTrustTheDefaults()
    {
        S3ClientFactory clientFactory = new ExposingS3ClientFactory();

        Properties props = new Properties();
        props.setProperty(ACCESS_KEY, "some_access_key");
        props.setProperty(SECRET_KEY, "super_secret_key");
        props.setProperty(REQUEST_METRIC_COLLECTOR_CLASS,
                          "org.carlspring.cloud.storage.s3fs.util.NoOpRequestMetricCollector");
        props.setProperty(CONNECTION_TIMEOUT, "10");
        props.setProperty(MAX_CONNECTIONS, "50");
        props.setProperty(MAX_ERROR_RETRY, "3");
        props.setProperty(PROTOCOL, "HTTP");
        props.setProperty(PROXY_DOMAIN, "localhost");
        props.setProperty(PROXY_HOST, "127.0.0.1");
        props.setProperty(PROXY_PASSWORD, "proxy_password");
        props.setProperty(PROXY_PORT, "12345");
        props.setProperty(PROXY_USERNAME, "proxy_username");
        props.setProperty(PROXY_WORKSTATION, "what.does.this.do.localhost");
        props.setProperty(SOCKET_SEND_BUFFER_SIZE_HINT, "48000");
        props.setProperty(SOCKET_RECEIVE_BUFFER_SIZE_HINT, "49000");
        props.setProperty(SOCKET_TIMEOUT, "30");
        props.setProperty(USER_AGENT, "I-am-Groot");
        props.setProperty(SIGNER_OVERRIDE, "S3SignerType");
        props.setProperty(PATH_STYLE_ACCESS, "true");
        props.setProperty(REGION, "eu-central-1");

        ExposingS3Client client =
                (ExposingS3Client) clientFactory.getS3Client(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);

        final AwsCredentialsProvider credentialsProvider = clientFactory.getCredentialsProvider(props);
        final AwsCredentials credentials = credentialsProvider.resolveCredentials();

        assertEquals("some_access_key", credentials.accessKeyId());
        assertEquals("super_secret_key", credentials.secretAccessKey());

        //TODO: Review how to get these attributes with v2
        /*
        final SdkHttpClient httpClient = clientFactory.getHttpClient(props);
        final ApacheHttpClient apacheHttpClient = (ApacheHttpClient) httpClient;

        assertEquals(10, clientConfiguration.getConnectionTimeout());
        assertEquals(50, clientConfiguration.getMaxConnections());
         */

        ClientOverrideConfiguration overrideConfiguration = clientFactory.getOverrideConfiguration(props);

        assertTrue(overrideConfiguration.retryPolicy().isPresent());
        assertEquals(3, overrideConfiguration.retryPolicy().get().numRetries());

        //TODO: Review how to get these attributes with v2
        //assertEquals(Protocol.HTTPS, overrideConfiguration.getProtocol());

        ProxyConfiguration proxyConfiguration = clientFactory.getProxyConfiguration(props);

        assertEquals("127.0.0.1", proxyConfiguration.host());
        assertEquals(12345, proxyConfiguration.port());
        assertEquals("proxy_username", proxyConfiguration.username());
        assertEquals("proxy_password", proxyConfiguration.password());
        assertEquals("localhost", proxyConfiguration.ntlmDomain());
        assertEquals("what.does.this.do.localhost", proxyConfiguration.ntlmWorkstation());

        //TODO: Review how to get these attributes with v2
        /*
        assertEquals(48000, clientConfiguration.getSocketBufferSizeHints()[0]);
        assertEquals(49000, clientConfiguration.getSocketBufferSizeHints()[1]);
        assertEquals(30, clientConfiguration.getSocketTimeout());
        assertEquals("I-am-Groot", clientConfiguration.getUserAgent());
        assertEquals("S3SignerType", clientConfiguration.getSignerOverride());
         */

        S3Configuration serviceConfiguration = clientFactory.getServiceConfiguration(props);
        assertTrue(serviceConfiguration.pathStyleAccessEnabled());
    }

    @Test
    void theDefaults()
    {
        S3ClientFactory clientFactory = new ExposingS3ClientFactory();

        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "giev.ma.access!");
        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "I'll never teeeeeellllll!");

        Properties props = new Properties();
        props.setProperty(REGION, "eu-central-1");

        final AwsCredentialsProvider credentialsProvider = clientFactory.getCredentialsProvider(props);
        final AwsCredentials credentials = credentialsProvider.resolveCredentials();

        assertEquals("giev.ma.access!", credentials.accessKeyId());
        assertEquals("I'll never teeeeeellllll!", credentials.secretAccessKey());

        //TODO: Review how to get these attributes with v2
        /*
        final SdkHttpClient httpClient = clientFactory.getHttpClient(props);
        final ApacheHttpClient apacheHttpClient = (ApacheHttpClient) httpClient;

        assertEquals(10, clientConfiguration.getConnectionTimeout());
        assertEquals(50, clientConfiguration.getMaxConnections());
         */

        ClientOverrideConfiguration overrideConfiguration = clientFactory.getOverrideConfiguration(props);

        assertFalse(overrideConfiguration.retryPolicy().isPresent());

        //TODO: Review how to get these attributes with v2
        //assertEquals(Protocol.HTTPS, overrideConfiguration.getProtocol());

        ProxyConfiguration proxyConfiguration = clientFactory.getProxyConfiguration(props);

        assertNull(proxyConfiguration.host());
        assertEquals(0, proxyConfiguration.port());
        assertNull(proxyConfiguration.username());
        assertNull(proxyConfiguration.password());
        assertNull(proxyConfiguration.ntlmDomain());
        assertNull(proxyConfiguration.ntlmWorkstation());

        //TODO: Review how to get these attributes with v2
        /*
        assertEquals(48000, clientConfiguration.getSocketBufferSizeHints()[0]);
        assertEquals(49000, clientConfiguration.getSocketBufferSizeHints()[1]);
        assertEquals(30, clientConfiguration.getSocketTimeout());
        assertEquals("I-am-Groot", clientConfiguration.getUserAgent());
        assertEquals("S3SignerType", clientConfiguration.getSignerOverride());
         */

        S3Configuration serviceConfiguration = clientFactory.getServiceConfiguration(props);
        assertFalse(serviceConfiguration.pathStyleAccessEnabled());
    }

    @Test
    void halfTheCredentials()
    {
        S3ClientFactory clientFactory = new ExposingS3ClientFactory();

        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "I'll never teeeeeellllll!");

        Properties props = new Properties();
        props.setProperty(ACCESS_KEY, "I want access");
        props.setProperty(REGION, "eu-central-1");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NullPointerException.class,
                                           () -> clientFactory.getS3Client(S3EndpointConstant.S3_GLOBAL_URI_TEST,
                                                                           props));

        assertNotNull(exception);
    }

    @Test
    void theOtherHalf()
    {
        S3ClientFactory clientFactory = new ExposingS3ClientFactory();

        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "I want access");

        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "I'll never teeeeeellllll!");
        props.setProperty(REGION, "eu-central-1");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(NullPointerException.class,
                                           () -> clientFactory.getS3Client(S3EndpointConstant.S3_GLOBAL_URI_TEST,
                                                                           props));

        assertNotNull(exception);
    }

    @Test
    void overrideHostAndPort()
    {
        S3ClientFactory clientFactory = new ExposingS3ClientFactory();

        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "test");
        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "test");

        URI uri = URI.create("s3://localhost:8001/");
        Properties props = new Properties();
        final String regionName = "eu-central-1";
        props.setProperty(REGION, regionName);
        final String protocol = "https";
        props.setProperty(PROTOCOL, protocol);
        ExposingS3Client client = (ExposingS3Client) clientFactory.getS3Client(uri, props);

        assertNotNull(client);

        final SdkClientConfiguration sdkClientConfiguration = client.getClientConfiguration();
        final URI endpoint = sdkClientConfiguration.option(ENDPOINT);

        assertEquals("https", endpoint.getScheme());
        assertEquals("localhost", endpoint.getHost());
        assertEquals(8001, endpoint.getPort());
    }

}
