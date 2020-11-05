package org.carlspring.cloud.storage.s3fs.path;

import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GetKeyTest
        extends S3UnitTestBase
{


    @BeforeEach
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        fileSystem = s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    private S3Path getPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Test
    void getKeyBucket()
    {
        S3Path path = getPath("/bucket");

        assertEquals("", path.getKey());
    }

    @Test
    void getKeyFile()
    {
        S3Path path = getPath("/bucket/file");

        assertEquals("file", path.getKey());
    }

    @Test
    void getKeyFolder()
    {
        S3Path path = getPath("/bucket/folder/");

        assertEquals("folder/", path.getKey());
    }

    @Test
    void getKeyParent()
    {
        S3Path path = (S3Path) getPath("/bucket/folder/file").getParent();

        assertEquals("folder/", path.getKey());
    }

    @Test
    void getKeyRoot()
    {
        S3Path path = (S3Path) getPath("/bucket/folder/file").getRoot();

        assertEquals("", path.getKey());
    }

    @Test
    void getKeyEncodingPath()
    {
        S3Path path = getPath("/bucket/path with spaces/to/β ϐ");

        assertEquals("path with spaces/to/β ϐ", path.getKey());
    }

}
