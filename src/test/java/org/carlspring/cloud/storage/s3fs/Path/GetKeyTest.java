package org.carlspring.cloud.storage.s3fs.Path;

import org.carlspring.cloud.storage.s3fs.S3FileSystemProvider;
import org.carlspring.cloud.storage.s3fs.S3Path;
import org.carlspring.cloud.storage.s3fs.S3UnitTestBase;
import org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import static org.carlspring.cloud.storage.s3fs.util.S3EndpointConstant.S3_GLOBAL_URI_TEST;
import static org.junit.Assert.assertEquals;

public class GetKeyTest
        extends S3UnitTestBase
{

    private S3FileSystemProvider s3fsProvider;


    @Before
    public void setup()
            throws IOException
    {
        s3fsProvider = getS3fsProvider();
        s3fsProvider.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
    }

    private S3Path getPath(String path)
    {
        return s3fsProvider.getFileSystem(S3_GLOBAL_URI_TEST).getPath(path);
    }

    @Test
    public void getKeyBucket()
    {
        S3Path path = getPath("/bucket");

        assertEquals("", path.getKey());
    }

    @Test
    public void getKeyFile()
    {
        S3Path path = getPath("/bucket/file");

        assertEquals(path.getKey(), "file");
    }

    @Test
    public void getKeyFolder()
    {
        S3Path path = getPath("/bucket/folder/");

        assertEquals(path.getKey(), "folder/");
    }

    @Test
    public void getKeyParent()
    {
        S3Path path = (S3Path) getPath("/bucket/folder/file").getParent();

        assertEquals(path.getKey(), "folder/");
    }

    @Test
    public void getKeyRoot()
    {
        S3Path path = (S3Path) getPath("/bucket/folder/file").getRoot();

        assertEquals(path.getKey(), "");
    }

    @Test
    public void getKeyEncodingPath()
    {
        S3Path path = getPath("/bucket/path with spaces/to/β ϐ");

        assertEquals("path with spaces/to/β ϐ", path.getKey());
    }

}
