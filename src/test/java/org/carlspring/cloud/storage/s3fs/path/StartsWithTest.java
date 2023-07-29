package org.carlspring.cloud.storage.s3fs.path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.Paths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StartsWithTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    @AfterEach
    public void tearDown()
            throws IOException
    {
        s3fsProvider.close((S3FileSystem) fileSystem);
        fileSystem.close();
    }

    private S3Path getPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Test
    void startsWithBucket()
    {
        assertTrue(getPath("/bucket/file1").startsWith(getPath("/bucket/")));
        assertFalse(getPath("/bucket/file1").startsWith(getPath("/bucket")));
    }

    @Test
    void startsWithBlank()
    {
        assertFalse(getPath("/bucket/file1").startsWith(getPath("")));
    }

    @Test
    void startsWithBlankRelative()
    {
        assertFalse(getPath("file1").startsWith(getPath("")));
    }

    @Test
    void startsWithSlash()
    {
        assertTrue(getPath("/bucket/file").startsWith(getPath("/bucket/")));
    }

    @Test
    void startsWithBlankBlank()
    {
        assertTrue(getPath("").startsWith(getPath("")));
    }

    @Test
    void startsWithOnlyBuckets()
    {
        assertTrue(getPath("/bucket").startsWith(getPath("/bucket")));
    }

    @Test
    void startsWithRelativeVsAbsolute()
    {
        assertFalse(getPath("/bucket/file1").startsWith(getPath("file1")));
    }

    @Test
    void startsWithRelativeVsAbsoluteInBucket()
    {
        assertFalse(getPath("/bucket/file1").startsWith(getPath("bucket")));
    }

    @Test
    void startsWithFalse()
    {
        assertFalse(getPath("/bucket/file1").startsWith(getPath("/bucket/file1/file2")));
        assertTrue(getPath("/bucket/file1/file2").startsWith(getPath("/bucket/file1")));
    }

    @Test
    void startsWithNotNormalize()
    {
        assertFalse(getPath("/bucket/file1/file2").startsWith(getPath("/bucket/file1/../")));
    }

    @Test
    void startsWithNormalize()
    {
        // in this implementation not exists .. or . special paths
        assertTrue(getPath("/bucket/file1/file2").startsWith(getPath("/bucket/file1/../").normalize()));
    }

    @Test
    void startsWithRelative()
    {
        assertTrue(getPath("file/file1").startsWith(getPath("file")));
    }

    @Test
    void startsWithDifferentProvider()
    {
        assertFalse(getPath("/bucket/hello").startsWith(Paths.get("/bucket")));
    }

    @Test
    void startsWithString()
    {
        assertTrue(getPath("/bucket/hello").startsWith("/bucket/hello"));
    }

    @Test
    void startsWithStringRelative()
    {
        assertTrue(getPath("subkey1/hello").startsWith("subkey1/hello"));
    }

    @Test
    void startsWithStringOnlyBuckets()
    {
        assertTrue(getPath("/bucket").startsWith("/bucket"));
    }

    @Test
    void startsWithStringRelativeVsAbsolute()
    {
        assertFalse(getPath("/bucket/file1").startsWith("file1"));
    }

    @Test
    void startsWithStringFalse()
    {
        assertFalse(getPath("/bucket/file1").startsWith("/bucket/file1/file2"));
        assertTrue(getPath("/bucket/file1/file2").startsWith("/bucket/file1"));
    }

    @Test
    void startsWithStringRelativeVsAbsoluteInBucket()
    {
        assertFalse(getPath("/bucket/file1").startsWith("bucket"));
    }

}
