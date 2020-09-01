package org.carlspring.cloud.storage.s3fs.FileSystemProvider;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.ACCESS_KEY;
import static org.carlspring.cloud.storage.s3fs.AmazonS3Factory.SECRET_KEY;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class GetFileSystemIT
{

    private S3FileSystemProvider provider;


    @BeforeEach
    public void setup()
            throws IOException
    {
        System.clearProperty(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS);
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

        // Don't override with system envs that we can have set, like Travis
        doReturn(false).when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(false).when(provider).overloadPropertiesWithSystemProps(any(Properties.class), anyString());
    }

    @Test
    public void getFileSystemWithSameEnvReturnSameFileSystem()
    {
        Map<String, Object> env = ImmutableMap.<String, Object>of("s3fs_access_key", "a", "s3fs_secret_key", "b");
        FileSystem fileSystem = provider.getFileSystem(S3_GLOBAL_URI_IT, env);

        assertNotNull(fileSystem);

        FileSystem sameFileSystem = provider.getFileSystem(S3_GLOBAL_URI_IT, env);

        assertSame(fileSystem, sameFileSystem);
    }

}
