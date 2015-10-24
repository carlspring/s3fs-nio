package com.upplication.s3fs;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Like {@link S3FileSystemProviderTest}, but without mocks.
 */
public class S3FileSystemProviderWithoutMocksTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() {
        s3fsProvider = new S3FileSystemProvider();
    }

    @Test
    public void getFileSystemWithEnv() {
        Map<String, Object> env = ImmutableMap.<String, Object> of("s3fs_access_key", "a", "s3fs_secret_key", "b");
        FileSystem fileSystem = s3fsProvider.getFileSystem(S3_GLOBAL_URI, env);
        assertNotNull(fileSystem);
        try {
            FileSystem sameFileSystem = s3fsProvider.getFileSystem(S3_GLOBAL_URI, env);
            assertSame(fileSystem, sameFileSystem);
        } catch (FileSystemAlreadyExistsException fsaee) {
            fail("A filesystem should not be created multiple times");
        }
    }
}
