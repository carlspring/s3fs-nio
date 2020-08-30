package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3FileSystem;
import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;
import java.nio.file.FileSystem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.*;

public class GetNameTest
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
    public void getNameBucket()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            // TODO: this is ok?
            S3Path path = getPath("/bucket");
            path.getName(0);
        });

        assertNotNull(exception);
    }

    @Test
    public void getName0()
    {
        S3Path path = getPath("/bucket/file");

        assertEquals(getPath("/bucket/file"), path.getName(0));
    }

    @Test
    public void getNames()
    {
        S3Path path = getPath("/bucket/path/to/file");

        assertEquals(path.getName(0), getPath("/bucket/path/"));
        assertEquals(path.getName(1), getPath("to/"));
        assertEquals(path.getName(2), getPath("file"));
    }

    @Test
    public void getNameOutOfIndex()
    {
        // We're expecting an exception here to be thrown
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            S3Path path = getPath("/bucket/path/to/file");
            path.getName(3);
        });

        assertNotNull(exception);
    }

}
