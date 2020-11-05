package org.carlspring.cloud.storage.s3fs.path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SubpathTest
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
    void subPath0()
    {
        assertEquals(getPath("/bucket/path/"), getPath("/bucket/path/to/file").subpath(0, 1));
    }

    @Test
    void subPath()
    {
        assertEquals(getPath("/bucket/path/to/"), getPath("/bucket/path/to/file").subpath(0, 2));
        assertEquals(getPath("/bucket/path/to/file"), getPath("/bucket/path/to/file").subpath(0, 3));
        assertEquals(getPath("to/"), getPath("/bucket/path/to/file").subpath(1, 2));
        assertEquals(getPath("to/file"), getPath("/bucket/path/to/file").subpath(1, 3));
        assertEquals(getPath("file"), getPath("/bucket/path/to/file").subpath(2, 3));
    }

    @Test
    void subPathOutOfRange()
    {
        S3Path path = getPath("/bucket/path/to/file");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class,
                                           () -> path.subpath(0, 4));

        assertNotNull(exception);
    }

}
