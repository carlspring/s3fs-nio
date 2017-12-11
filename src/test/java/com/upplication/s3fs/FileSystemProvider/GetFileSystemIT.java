package com.upplication.s3fs.FileSystemProvider;

import com.google.common.collect.ImmutableMap;
import com.upplication.s3fs.S3FileSystemProvider;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.Properties;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_IT;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class GetFileSystemIT {

    private S3FileSystemProvider provider;

    @Before
    public void setup() throws IOException {
        System.clearProperty(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS);
        System.clearProperty(ACCESS_KEY);
        System.clearProperty(SECRET_KEY);
        try {
            FileSystems.getFileSystem(S3_GLOBAL_URI_IT).close();
        } catch (FileSystemNotFoundException e) {
            // ignore this
        }
        provider = spy(new S3FileSystemProvider());
        // dont override with system envs that we can have setted, like travis
        doReturn(false).when(provider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(false).when(provider).overloadPropertiesWithSystemProps(any(Properties.class), anyString());
    }

    @Test
    public void getFileSystemWithSameEnvReturnSameFileSystem() {
        Map<String, Object> env = ImmutableMap.<String, Object>of("s3fs_access_key", "a", "s3fs_secret_key", "b");
        FileSystem fileSystem = provider.getFileSystem(S3_GLOBAL_URI_IT, env);
        assertNotNull(fileSystem);

        FileSystem sameFileSystem = provider.getFileSystem(S3_GLOBAL_URI_IT, env);
        assertSame(fileSystem, sameFileSystem);
    }
}