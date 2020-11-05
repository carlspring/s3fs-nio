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

class GetNameTest
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
    void getNameBucket()
    {
        S3Path path = getPath("/bucket");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> path.getName(0));

        assertNotNull(exception);
    }

    @Test
    void getName0()
    {
        S3Path path = getPath("/bucket/file");

        assertEquals(getPath("/bucket/file"), path.getName(0));
    }

    @Test
    void getNames()
    {
        S3Path path = getPath("/bucket/path/to/file");

        assertEquals(path.getName(0), getPath("/bucket/path/"));
        assertEquals(path.getName(1), getPath("to/"));
        assertEquals(path.getName(2), getPath("file"));
    }

    @Test
    void getNameOutOfIndex()
    {
        S3Path path = getPath("/bucket/path/to/file");

        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> path.getName(3));

        assertNotNull(exception);
    }

}
