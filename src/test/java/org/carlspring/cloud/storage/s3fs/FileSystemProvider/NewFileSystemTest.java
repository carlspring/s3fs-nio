package org.carlspring.cloud.storage.s3fs.FileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemConfigurationException;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.amazonaws.services.s3.internal.Constants;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS;
import static org.carlspring.cloud.storage.s3fs.S3FileSystemProvider.CHARSET_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class NewFileSystemTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @Test
    public void misconfigure()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(S3FileSystemConfigurationException.class, () -> {
            Properties props = new Properties();
            props.setProperty(AMAZON_S3_FACTORY_CLASS, "org.carlspring.cloud.storage.s3fs.util.BrokenAmazonS3Factory");

            s3fsProvider.createFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, props);
        });

        assertNotNull(exception);
    }

    @Test
    public void newS3FileSystemWithEmptyHostAndUserInfo()
    {
        FileSystem s3fs = s3fsProvider.newFileSystem(URI.create("s3:///bucket/file"),
                                                     ImmutableMap.<String, Object>of());

        assertEquals(Constants.S3_HOSTNAME, ((S3FileSystem) s3fs).getKey());
    }

    @Test
    public void newS3FileSystemWithEmptyHost()
    {
        FileSystem s3fs = s3fsProvider.newFileSystem(URI.create("s3://access-key:secret-key@/bucket/file"),
                                                     ImmutableMap.<String, Object>of());

        assertEquals("access-key:secret-key@" + Constants.S3_HOSTNAME, ((S3FileSystem) s3fs).getKey());
    }

    @Test
    public void newS3FileSystemWithCustomHost()
    {
        FileSystem s3fs = s3fsProvider.newFileSystem(URI.create("s3://access-key:secret-key@my.ceph.storage"),
                                                     ImmutableMap.<String, Object>of());

        assertEquals("access-key:secret-key@my.ceph.storage", ((S3FileSystem) s3fs).getKey());
    }

    @Test
    public void newS3FileSystemWithCustomHostAndBucket()
    {
        FileSystem s3fs = s3fsProvider.newFileSystem(URI.create("s3://access-key:secret-key@my.ceph.storage/bucket"),
                                                     ImmutableMap.<String, Object>of());

        assertEquals("access-key:secret-key@my.ceph.storage", ((S3FileSystem) s3fs).getKey());
    }

    @Test
    public void createsAuthenticatedByEnv()
    {
        Map<String, ?> env = buildFakeEnv();

        URI uri = URI.create("s3://" + UUID.randomUUID().toString());

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, env);

        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri),
                                              eq(buildFakeProps((String) env.get(ACCESS_KEY),
                                                                (String) env.get(SECRET_KEY))));
    }

    @Test
    public void setEncodingByProperties()
    {
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "better_secret_key");
        props.setProperty(ACCESS_KEY, "better_access_key");
        props.setProperty(CHARSET_KEY, "UTF-8");

        doReturn(props).when(s3fsProvider).loadAmazonProperties();

        URI uri = URI.create("s3://" + UUID.randomUUID().toString());

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());

        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri),
                                              eq(buildFakeProps("better_access_key", "better_secret_key", "UTF-8")));
    }

    @Test
    public void createAuthenticatedByProperties()
    {
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "better_secret_key");
        props.setProperty(ACCESS_KEY, "better_access_key");

        doReturn(props).when(s3fsProvider).loadAmazonProperties();

        URI uri = URI.create("s3://" + UUID.randomUUID().toString());

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());

        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps("better_access_key", "better_secret_key")));
    }

    @Test
    public void createAuthenticatedBySystemEnvironment()
    {
        final String accessKey = "better-access-key";
        final String secretKey = "better-secret-key";

        URI uri = URI.create("s3://" + UUID.randomUUID().toString());

        doReturn(accessKey).when(s3fsProvider).systemGetEnv(ACCESS_KEY);
        doReturn(secretKey).when(s3fsProvider).systemGetEnv(SECRET_KEY);
        doCallRealMethod().when(s3fsProvider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());

        s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());

        verify(s3fsProvider).createFileSystem(eq(uri), argThat(new ArgumentMatcher<Properties>()
        {
            @Override
            public boolean matches(Properties properties)
            {
                assertEquals(accessKey, properties.getProperty(ACCESS_KEY));
                assertEquals(secretKey, properties.getProperty(SECRET_KEY));

                return true;
            }
        }));
    }

    @Test
    public void createsAnonymous()
    {
        URI uri = URI.create("s3://" + UUID.randomUUID().toString());

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());

        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri), eq(buildFakeProps(null, null)));
    }

    @Test
    public void createWithDefaultEndpoint()
    {
        Properties props = new Properties();
        props.setProperty(SECRET_KEY, "better_secret_key");
        props.setProperty(ACCESS_KEY, "better_access_key");
        props.setProperty(CHARSET_KEY, "UTF-8");

        doReturn(props).when(s3fsProvider).loadAmazonProperties();

        URI uri = URI.create("s3:///");

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());

        assertNotNull(fileSystem);

        verify(s3fsProvider).createFileSystem(eq(uri),
                                              eq(buildFakeProps("better_access_key", "better_secret_key", "UTF-8")));
    }

    @Test
    public void createWithOnlyAccessKey()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Properties props = new Properties();

            URI uri = URI.create("s3://" + UUID.randomUUID().toString());

            props.setProperty(ACCESS_KEY, "better_access_key");

            doReturn(props).when(s3fsProvider).loadAmazonProperties();

            s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());
        });

        assertNotNull(exception);
    }

    @Test
    public void createWithOnlySecretKey()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Properties props = new Properties();
            props.setProperty(SECRET_KEY, "better_secret_key");

            doReturn(props).when(s3fsProvider).loadAmazonProperties();

            URI uri = URI.create("s3://" + UUID.randomUUID().toString());

            s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());
        });

        assertNotNull(exception);
    }

    @Test
    public void createFailsIfAlreadyCreated()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemAlreadyExistsException.class, () -> {
            FileSystem fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST,
                                                               ImmutableMap.<String, Object>of());

            assertNotNull(fileSystem);

            URI uri = URI.create("s3://" + UUID.randomUUID().toString());

            s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());
        });

        assertNotNull(exception);
    }

    @Test
    public void createWithWrongEnv()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            Map<String, Object> env = ImmutableMap.<String, Object>builder().put(ACCESS_KEY, 1234)
                                                                            .put(SECRET_KEY,
                                                                                 "secret key")
                                                                            .build();

            URI uri = URI.create("s3://" + UUID.randomUUID().toString());

            FileSystem fileSystem = s3fsProvider.newFileSystem(uri, env);

            assertNotNull(fileSystem);

            s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());
        });

        assertNotNull(exception);
    }

    @Test
    public void getFileSystem()
    {
        URI uri = URI.create("s3://" + UUID.randomUUID().toString());

        FileSystem fileSystem = s3fsProvider.newFileSystem(uri, ImmutableMap.<String, Object>of());

        assertNotNull(fileSystem);

        fileSystem = s3fsProvider.getFileSystem(uri, ImmutableMap.<String, Object>of());

        assertNotNull(fileSystem);

        FileSystem other = s3fsProvider.getFileSystem(uri);

        assertSame(fileSystem, other);
    }

    @Test
    public void getUnknownFileSystem()
    {
        FileSystem fileSystem = s3fsProvider.getFileSystem(URI.create("s3://endpoint20/bucket/path/to/file"),
                                                           ImmutableMap.<String, Object>of());

        assertNotNull(fileSystem);
    }

    @Test
    public void closeFileSystemReturnNewFileSystem()
            throws IOException
    {
        S3FileSystemProvider provider = new S3FileSystemProvider();

        Map<String, ?> env = buildFakeEnv();

        FileSystem fileSystem = provider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);

        assertNotNull(fileSystem);

        fileSystem.close();

        FileSystem fileSystem2 = provider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);

        assertNotSame(fileSystem, fileSystem2);
    }

    @Test
    public void createTwoFileSystemThrowError()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(FileSystemAlreadyExistsException.class, () -> {
            S3FileSystemProvider provider = new S3FileSystemProvider();

            Map<String, ?> env = buildFakeEnv();

            FileSystem fileSystem = provider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);

            assertNotNull(fileSystem);

            provider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, env);
        });

        assertNotNull(exception);
    }


    private Map<String, ?> buildFakeEnv()
    {
        return ImmutableMap.<String, Object>builder().put(ACCESS_KEY, "access_key")
                                                     .put(SECRET_KEY, "secret_key")
                                                     .build();
    }

    private Properties buildFakeProps(String access_key, String secret_key, String encoding)
    {
        Properties props = buildFakeProps(access_key, secret_key);
        props.setProperty(CHARSET_KEY, encoding);

        return props;
    }


    private Properties buildFakeProps(String access_key, String secret_key)
    {
        Properties props = new Properties();
        props.setProperty(AMAZON_S3_FACTORY_CLASS, "org.carlspring.cloud.storage.s3fs.util.AmazonS3MockFactory");

        if (access_key != null)
        {
            props.setProperty(ACCESS_KEY, access_key);
        }

        if (secret_key != null)
        {
            props.setProperty(SECRET_KEY, secret_key);
        }

        return props;
    }

}
