package org.carlspring.cloud.storage.s3fs.fileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.BaseIntegrationTest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.S3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.S3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@S3IntegrationTest
class NewFileSystemIT extends BaseIntegrationTest
{

    private S3FileSystemProvider provider;


    @BeforeEach
    public void setup()
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.S3_FACTORY_CLASS);
        System.clearProperty(ACCESS_KEY);
        System.clearProperty(SECRET_KEY);

        try
        {
            FileSystems.getFileSystem(S3_GLOBAL_URI_IT).close();
        }
        catch (FileSystemNotFoundException e)
        {
            // ignore this
        }

        provider = spy(new S3FileSystemProvider());

        doReturn(buildFakeProps()).when(provider).loadAmazonProperties();

        // Don't override with system envs that we can have set, like Travis
        doReturn(false).when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(false).when(provider).overloadPropertiesWithSystemProps(any(Properties.class), anyString());
    }

    @Test
    void createAuthenticatedByProperties()
    {
        URI uri = URI.create("s3://yadi/");

        FileSystem fileSystem = provider.newFileSystem(uri, null);

        assertNotNull(fileSystem);

        verify(provider).createFileSystem(eq(uri), eq(buildFakeProps()));
    }

    @Test
    void createsAuthenticatedByEnvOverridesProps()
    {
        final Map<String, String> env = buildFakeEnv();

        provider.newFileSystem(S3_GLOBAL_URI_IT, env);

        verifyCreationWithParams(env.get(ACCESS_KEY), env.get(SECRET_KEY), S3_GLOBAL_URI_IT);
    }

    @Test
    void createsAuthenticatedBySystemProps()
    {
        doCallRealMethod().when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());

        final String propAccessKey = "env-access-key";
        final String propSecretKey = "env-secret-key";

        doReturn(propAccessKey).when(provider).systemGetEnv(eq(ACCESS_KEY));
        doReturn(propSecretKey).when(provider).systemGetEnv(eq(SECRET_KEY));

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI_IT, null);

        assertNotNull(fileSystem);

        verifyCreationWithParams(propAccessKey, propSecretKey, S3_GLOBAL_URI_IT);
    }

    @Test
    void createsAuthenticatedBySystemEnv()
    {
        doCallRealMethod().when(provider).overloadPropertiesWithSystemProps(any(Properties.class), anyString());

        final String propAccessKey = "prop-access-key";
        final String propSecretKey = "prop-secret-key";

        System.setProperty(ACCESS_KEY, propAccessKey);
        System.setProperty(SECRET_KEY, propSecretKey);

        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI_IT, null);

        assertNotNull(fileSystem);

        verifyCreationWithParams(propAccessKey, propSecretKey, S3_GLOBAL_URI_IT);
    }

    @Test
    void createsAuthenticatedByUri()
    {
        final String accessKeyUri = "access-key-uri";
        final String secretKeyUri = "secret-key-uri";

        URI uri = URI.create("s3://" + accessKeyUri + ":" + secretKeyUri + "@s3.amazon.com");

        provider.newFileSystem(uri, null);

        verifyCreationWithParams(accessKeyUri, secretKeyUri, uri);
    }


    @Test
    void createsAnonymousNotPossible()
    {
        FileSystem fileSystem = provider.newFileSystem(S3_GLOBAL_URI_IT, ImmutableMap.of());

        assertNotNull(fileSystem);

        verify(provider).createFileSystem(eq(S3_GLOBAL_URI_IT), eq(buildFakeProps()));
    }

    private Map<String, String> buildFakeEnv()
    {
        return ImmutableMap.<String, String>builder().put(ACCESS_KEY, "access-key")
                                                     .put(SECRET_KEY, "secret-key")
                                                     .build();
    }

    private void verifyCreationWithParams(final String accessKeyUri, final String secretKeyUri, URI uri)
    {
        verify(provider).createFileSystem(eq(uri), argThat(properties -> {
            assertEquals(accessKeyUri, properties.get(ACCESS_KEY));
            assertEquals(secretKeyUri, properties.get(SECRET_KEY));

            return true;
        }));
    }

    private Properties buildFakeProps()
    {
        try
        {
            Properties props = new Properties();
            props.load(Thread.currentThread()
                             .getContextClassLoader()
                             .getResourceAsStream("amazon-test-sample.properties"));

            return props;
        }
        catch (IOException e)
        {
            throw new RuntimeException("amazon-test-sample.properties not present");
        }
    }

}
